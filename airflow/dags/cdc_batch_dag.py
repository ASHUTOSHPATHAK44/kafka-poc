"""
Airflow DAG: run the batch CDC consumer every 5 minutes.

This DAG does NOT run Spark inside Airflow. It triggers one bounded run of
batch_consumer.py by exec-ing `spark-submit` inside the already-running
poc-spark-client container (via the mounted host Docker socket).

Each run:
  * reads new Kafka records from each table's consumer-group offset to the
    current end (bounded by max_records_per_run),
  * MERGEs them into lakehouse.cdc_batch.<table>,
  * commits offsets to Kafka only on success.

`catchup=False` + `max_active_runs=1` ensure runs don't overlap or backfill —
each schedule tick processes whatever has accumulated since the last run.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CLIENT_CONTAINER = "poc-spark-client"

# spark-submit the batch job inside the spark-client container. Capped at 1 core
# so it coexists with the streaming job on the 2-core worker.
SUBMIT_CMD = (
    f"docker exec {SPARK_CLIENT_CONTAINER} /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.cores.max=1 "
    "/opt/spark/jobs/batch_consumer.py"
)

default_args = {
    "owner": "poc",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="cdc_batch_to_iceberg",
    description="Batch-consume Debezium CDC from Kafka and MERGE into Iceberg (cdc_batch).",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",      # every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "kafka", "iceberg", "batch"],
) as dag:

    run_batch_consumer = BashOperator(
        task_id="run_batch_consumer",
        bash_command=SUBMIT_CMD,
    )
