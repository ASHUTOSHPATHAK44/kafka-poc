"""
Smoke test for the consumer stack.

Verifies, end to end:
  1. the Spark standalone cluster runs a job (master + worker),
  2. the Iceberg REST catalog is reachable,
  3. Spark can CREATE / INSERT / SELECT an Iceberg table, and
  4. the table's data + metadata actually land in MinIO (S3).

Catalog "lakehouse", its REST uri, warehouse and S3/MinIO settings all come
from /opt/spark/conf/spark-defaults.conf, so this script stays config-free.

Run with spark-submit (see the command in chat).
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("iceberg-smoke-test")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DB = "lakehouse.poc_smoke"
TABLE = f"{DB}.ping"

print(">>> Spark version:", spark.version)
print(">>> Default catalog:", spark.conf.get("spark.sql.defaultCatalog"))

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DB}")

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(
    f"""
    CREATE TABLE {TABLE} (
        id       BIGINT,
        label    STRING,
        ts       TIMESTAMP
    ) USING iceberg
    """
)

spark.sql(
    f"""
    INSERT INTO {TABLE} VALUES
        (1, 'hello-iceberg', current_timestamp()),
        (2, 'from-spark',    current_timestamp()),
        (3, 'into-minio',    current_timestamp())
    """
)

print(">>> Rows in", TABLE)
spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").show(truncate=False)

count = spark.sql(f"SELECT count(*) AS c FROM {TABLE}").collect()[0]["c"]
print(f">>> Row count = {count}")

print(">>> Table files (proves data is written to MinIO):")
spark.sql(f"SELECT file_path, record_count FROM {TABLE}.files").show(truncate=False)

assert count == 3, f"expected 3 rows, got {count}"
print(">>> SMOKE TEST PASSED")

spark.stop()
