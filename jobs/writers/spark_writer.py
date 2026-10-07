"""
Spark batch writer for the batch consumer.

Given a table's config and a resolved Kafka offset range, this:
  1. does a BATCH read of exactly that offset range (spark.read, not readStream),
  2. parses the Debezium envelope,
  3. dedups to the latest event per key within the batch,
  4. MERGEs into the Iceberg table (upsert + delete), identical semantics to the
     streaming job, just driven by explicit start/end offsets instead of a
     streaming checkpoint.

The SparkSession is created lazily and reused across tables in one run.
Offset *resolution* and *commit* happen in the consumer (via a Kafka client);
this writer only consumes the range it is told to.
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType, BooleanType,
)

KAFKA_BOOTSTRAP = "kafka:9092"

SPARK_TYPES = {
    "STRING": StringType(),
    "BIGINT": LongType(),
    "LONG": LongType(),
    "INT": IntegerType(),
    "INTEGER": IntegerType(),
    "DOUBLE": DoubleType(),
    "BOOLEAN": BooleanType(),
}


def _after_schema(columns):
    return StructType([StructField(n, SPARK_TYPES[t.upper()]) for n, t in columns])


def _ddl(columns):
    return ", ".join(f"{n} {t.upper()}" for n, t in columns)


def _envelope_schema(after_schema):
    return StructType([
        StructField("before", after_schema),
        StructField("after", after_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ])


class SparkWriter:
    """Spark-backed implementation of the batch writer."""

    def __init__(self):
        self._spark = None

    # -- session -------------------------------------------------------------
    @property
    def spark(self):
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName("cdc-batch-consumer")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
        return self._spark

    def ensure_namespace(self, namespace):
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    def stop(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    # -- the batch write -----------------------------------------------------
    def write_batch(self, name, cfg, namespace, starting_offsets, ending_offsets):
        """
        Read [starting_offsets, ending_offsets) for this table's topic and MERGE.

        starting_offsets / ending_offsets are Spark-Kafka JSON offset specs, e.g.
          {"poc.inventory.customers": {"0": 400}}
          {"poc.inventory.customers": {"0": 650}}
        Returns the number of change rows merged (post-dedup).
        """
        columns = cfg["columns"]
        cols = [c[0] for c in columns]
        key_col = cfg["key"]
        topic = cfg["topic"]
        table_fqn = f"{namespace}.{name}"
        spark = self.spark

        # Target Iceberg table (mirror of the Postgres table).
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn} ({_ddl(columns)}) USING iceberg")

        # Bounded BATCH read of exactly the resolved offset range.
        raw = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", json.dumps(starting_offsets))
            .option("endingOffsets", json.dumps(ending_offsets))
            .load()
        )

        if raw.rdd.isEmpty():
            print(f">>> {table_fqn}: no new records in range; nothing to merge")
            return 0

        envelope = _envelope_schema(_after_schema(columns))
        parsed = (
            raw.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), envelope).alias("e"))
            .where(col("e.op").isNotNull())
            .select("e.op", "e.ts_ms", "e.before", "e.after")
        )

        changes = parsed.selectExpr(
            "op", "ts_ms", "coalesce(after, before) AS row"
        ).select("op", "ts_ms", "row.*")

        changes.createOrReplaceTempView("batch_changes_raw")
        col_list = ", ".join(cols)
        deduped = spark.sql(f"""
            SELECT op, {col_list}
            FROM (
                SELECT op, {col_list},
                       row_number() OVER (
                           PARTITION BY {key_col}
                           ORDER BY ts_ms DESC
                       ) AS rn
                FROM batch_changes_raw
            )
            WHERE rn = 1
        """)
        deduped.createOrReplaceTempView("batch_changes")

        set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols)
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"s.{c}" for c in cols)

        # Show the exact records being written to the lakehouse this run
        # (the deduped change set: op + all columns), for readable logs.
        n = deduped.count()
        print(f"\n>>> ===== {table_fqn}: writing {n} record(s) this run "
              f"(range {starting_offsets} -> {ending_offsets}) =====")
        deduped.orderBy(key_col).show(n=min(n, 100), truncate=False)

        spark.sql(f"""
            MERGE INTO {table_fqn} AS t
            USING batch_changes AS s
            ON t.{key_col} = s.{key_col}
            WHEN MATCHED AND s.op = 'd'  THEN DELETE
            WHEN MATCHED AND s.op <> 'd' THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED AND s.op <> 'd' THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        print(f">>> {table_fqn}: MERGE complete — {n} change(s) applied "
              f"(c/r/u = upsert, d = delete)\n")
        return n
