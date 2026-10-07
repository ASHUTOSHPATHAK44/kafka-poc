"""
Generic, config-driven CDC -> Iceberg streaming engine (v2).

Same behaviour as cdc_to_iceberg.py (Option A: upsert + delete via MERGE INTO),
but every table is described in an external config file (tables.json) instead of
being hard-coded. Adding a table = one JSON entry + restart. No code change.

Design:
  * Each table declares its columns ONCE (name + Spark type). From that single
    list we derive BOTH the JSON parse schema and the Iceberg CREATE TABLE DDL,
    so the two can never drift apart (the old updated_at = NULL bug).
  * The engine loops over the config and, per table, wires up:
        Kafka read -> parse Debezium envelope -> per-batch dedup -> MERGE -> checkpoint
  * Runs independently of the v1 job: separate namespace + separate checkpoints,
    so it will not touch cdc_to_iceberg.py's tables or state.

Catalog / warehouse / MinIO settings come from
/opt/spark/conf/spark-defaults.conf (catalog name = "lakehouse").

Config path may be overridden as the first CLI arg; defaults to the tables.json
sitting next to this script.

Run with spark-submit (see the command in chat).
"""

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType, BooleanType,
)

KAFKA_BOOTSTRAP = "kafka:9092"
CATALOG = "lakehouse"
NAMESPACE = f"{CATALOG}.cdc_v2"                 # separate from v1's lakehouse.cdc
CHECKPOINT_ROOT = "/opt/spark/checkpoints_v2"   # separate from v1's checkpoints

# Map the type strings used in tables.json to Spark types (for the parse schema).
# The same string is emitted verbatim into the Iceberg DDL.
SPARK_TYPES = {
    "STRING": StringType(),
    "BIGINT": LongType(),
    "LONG": LongType(),
    "INT": IntegerType(),
    "INTEGER": IntegerType(),
    "DOUBLE": DoubleType(),
    "BOOLEAN": BooleanType(),
}


def load_config(path):
    with open(path, "r") as f:
        cfg = json.load(f)
    defaults = cfg.get("defaults", {})
    tables = cfg.get("tables", {})
    if not tables:
        raise ValueError(f"No 'tables' defined in config: {path}")
    return defaults, tables


def build_after_schema(columns):
    """columns = [[name, type], ...] -> StructType for the after/before image."""
    fields = []
    for name, type_str in columns:
        spark_type = SPARK_TYPES.get(type_str.upper())
        if spark_type is None:
            raise ValueError(
                f"Unknown type '{type_str}' for column '{name}'. "
                f"Supported: {sorted(SPARK_TYPES)}"
            )
        fields.append(StructField(name, spark_type))
    return StructType(fields)


def build_ddl(columns):
    """columns = [[name, type], ...] -> 'name TYPE, name TYPE, ...' for CREATE TABLE."""
    return ", ".join(f"{name} {type_str.upper()}" for name, type_str in columns)


def debezium_envelope_schema(after_schema):
    """Debezium value envelope: { before, after, op, ts_ms }."""
    return StructType([
        StructField("before", after_schema),
        StructField("after", after_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ])


def make_foreach_batch(table_fqn, key_col, cols):
    """Build a foreachBatch handler that MERGEs one micro-batch into Iceberg."""

    def upsert(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Use the batch's own session (streaming foreachBatch runs in a
        # different session than the one that started the query).
        batch_spark = batch_df.sparkSession

        # Deletes carry the key in `before`, everything else in `after`.
        changes = batch_df.selectExpr(
            "op",
            "ts_ms",
            "coalesce(after, before) AS row",
        ).select("op", "ts_ms", "row.*")
        # Collapse multiple events for the same key in one batch to the latest.
        changes.createOrReplaceTempView("changes_raw")
        col_list = ", ".join(cols)
        deduped = batch_spark.sql(f"""
            SELECT op, {col_list}
            FROM (
                SELECT op, {col_list},
                       row_number() OVER (
                           PARTITION BY {key_col}
                           ORDER BY ts_ms DESC
                       ) AS rn
                FROM changes_raw
            )
            WHERE rn = 1
        """)
        deduped.createOrReplaceTempView("changes")

        set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols)
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"s.{c}" for c in cols)

        batch_spark.sql(f"""
            MERGE INTO {table_fqn} AS t
            USING changes AS s
            ON t.{key_col} = s.{key_col}
            WHEN MATCHED AND s.op = 'd' THEN DELETE
            WHEN MATCHED AND s.op <> 'd' THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED AND s.op <> 'd' THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        print(f">>> batch {batch_id}: merged change(s) into {table_fqn}")

    return upsert


def start_table_stream(spark, name, cfg, defaults):
    """Wire up one table's Kafka -> Iceberg streaming query from its config."""
    columns = cfg["columns"]
    cols = [c[0] for c in columns]
    key_col = cfg["key"]
    topic = cfg["topic"]
    table_fqn = f"{NAMESPACE}.{name}"

    # Per-table settings fall back to the global defaults.
    starting_offsets = cfg.get("starting_offsets", defaults.get("starting_offsets", "earliest"))
    max_offsets = cfg.get("max_offsets_per_trigger", defaults.get("max_offsets_per_trigger"))
    processing_time = cfg.get("processing_time", defaults.get("processing_time"))

    after_schema = build_after_schema(columns)
    ddl = build_ddl(columns)

    # Target Iceberg table (mirror of the Postgres table).
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn} ({ddl}) USING iceberg")

    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
    )
    if max_offsets is not None:
        reader = reader.option("maxOffsetsPerTrigger", int(max_offsets))
    raw = reader.load()

    envelope = debezium_envelope_schema(after_schema)
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), envelope).alias("e"))
        .where(col("e.op").isNotNull())  # skip null/tombstone payloads
        .select("e.before", "e.after", "e.op", "e.ts_ms")
    )

    writer = (
        parsed.writeStream
        .foreachBatch(make_foreach_batch(table_fqn, key_col, cols))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{name}")
        .outputMode("update")
        .queryName(f"cdc-v2-{name}")
    )
    if processing_time:
        writer = writer.trigger(processingTime=processing_time)

    query = writer.start()
    trig = f"every {processing_time}" if processing_time else "default (back-to-back)"
    cap = max_offsets if max_offsets is not None else "unlimited"
    print(f">>> streaming {topic} -> {table_fqn}  [offsets={starting_offsets}, maxPerTrigger={cap}, trigger={trig}]")
    return query


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else \
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "tables.json")

    defaults, tables = load_config(config_path)

    spark = (
        SparkSession.builder
        .appName("kafka-to-iceberg-v2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f">>> config: {config_path}")
    print(f">>> tables: {', '.join(tables)}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

    queries = [start_table_stream(spark, name, cfg, defaults)
               for name, cfg in tables.items()]

    print(f">>> {len(queries)} streaming query(ies) started; awaiting changes...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
