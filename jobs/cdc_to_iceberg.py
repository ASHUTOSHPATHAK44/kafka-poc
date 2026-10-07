"""
CDC -> Iceberg streaming job (Option A: full upsert + delete via MERGE INTO).

Pipeline:
    Kafka (Debezium CDC topics)  ->  Spark Structured Streaming  ->  Iceberg (MinIO)

For each Debezium topic we:
  1. read it as a streaming source from Kafka,
  2. parse the Debezium JSON envelope (before / after / op),
  3. per micro-batch, MERGE the changes into an Iceberg table so the table
     always mirrors the current state of the Postgres table:
        - op = c/r/u  -> row present in `after`  -> INSERT or UPDATE (upsert)
        - op = d      -> row present in `before` -> DELETE

Catalog / warehouse / MinIO settings come from
/opt/spark/conf/spark-defaults.conf (catalog name = "lakehouse").

Run with spark-submit (see the command in chat).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType,
)

KAFKA_BOOTSTRAP = "kafka:9092"
CATALOG = "lakehouse"
NAMESPACE = f"{CATALOG}.cdc"

# --- per-table stream definitions -------------------------------------------
# `after_schema` describes the columns as Debezium emits them in the JSON
# `after` / `before` objects (schemas.enable=false, decimal.handling=double,
# time.precision=connect => timestamps arrive as epoch-millis longs).
TABLES = {
    "customers": {
        "topic": "poc.inventory.customers",
        "key": "id",
        "after_schema": StructType([
            StructField("id", LongType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("updated_at", LongType()),
        ]),
        "ddl": """
            id          BIGINT,
            first_name  STRING,
            last_name   STRING,
            email       STRING,
            updated_at  BIGINT
        """,
    },
    "orders": {
        "topic": "poc.inventory.orders",
        "key": "id",
        "after_schema": StructType([
            StructField("id", LongType()),
            StructField("customer_id", IntegerType()),
            StructField("product", StringType()),
            StructField("quantity", IntegerType()),
            StructField("amount", DoubleType()),
            StructField("created_at", LongType()),
        ]),
        "ddl": """
            id           BIGINT,
            customer_id  INT,
            product      STRING,
            quantity     INT,
            amount       DOUBLE,
            created_at   BIGINT
        """,
    },
}


def debezium_envelope_schema(after_schema: StructType) -> StructType:
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

        # Use the batch's own session — the streaming DataFrame passed into
        # foreachBatch belongs to a different session than the one used to
        # start the query, so temp views must be created via this session.
        batch_spark = batch_df.sparkSession

        # Resolve each change to a single desired end-state per key:
        #   deletes carry the key in `before`, everything else in `after`.
        changes = batch_df.selectExpr(
            "op",
            "ts_ms",
            "coalesce(after, before) AS row",
        ).select("op", "ts_ms", "row.*")

        # If a key appears multiple times in one batch, keep the last event
        # (Debezium ts_ms gives event order; break ties on the offset-like ts).
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


def main():
    spark = (
        SparkSession.builder
        .appName("cdc-to-iceberg")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

    queries = []
    for name, cfg in TABLES.items():
        table_fqn = f"{NAMESPACE}.{name}"
        cols = [f.name for f in cfg["after_schema"].fields]

        # Target Iceberg table (mirror of the Postgres table).
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn} ({cfg['ddl']}) USING iceberg")

        # Streaming source: read the Debezium topic from the beginning.
        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", cfg["topic"])
            .option("startingOffsets", "earliest")
            .load()
        )

        envelope = debezium_envelope_schema(cfg["after_schema"])
        parsed = (
            raw.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), envelope).alias("e"))
            # Debezium sends periodic null/heartbeat-ish tombstones; keep real ops.
            .where(col("e.op").isNotNull())
            .select("e.before", "e.after", "e.op", "e.ts_ms")
        )

        query = (
            parsed.writeStream
            .foreachBatch(make_foreach_batch(table_fqn, cfg["key"], cols))
            .option("checkpointLocation", f"/opt/spark/checkpoints/{name}")
            .outputMode("update")
            .queryName(f"cdc-{name}")
            .start()
        )
        queries.append(query)
        print(f">>> streaming {cfg['topic']} -> {table_fqn}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
