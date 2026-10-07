"""
Airflow DAG: random CDC data generator.

Each run inserts ONE random customer and ONE random order into Postgres (via
`docker exec` into the postgres container, using the FK-safe random SQL in
scripts/generate_data.sql). Those changes flow Debezium -> Kafka -> Iceberg.

Control model (what the user asked for):
  * ENABLE (unpause) the DAG  -> it fires every minute -> records keep inserting
  * DISABLE (pause) the DAG   -> no new runs -> insertion stops
  * RE-ENABLE                 -> it resumes inserting

This uses the "short task, 1-minute schedule" pattern (one insert per run)
rather than a single long-running loop, so pausing stops it promptly and a
run never overlaps the next.

Requires scripts/generate_data.sql to be available to the Airflow container.
It is mounted read-only at /opt/airflow/scripts (see docker-compose.yml).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

POSTGRES_CONTAINER = "poc-postgres"
# Mounted into the Airflow container alongside the dags (see compose).
SQL_PATH = "/opt/airflow/scripts/generate_data.sql"

# Pipe the SQL file into psql inside the postgres container. Aligned output
# (no -q/-t) so the RETURNING rows and \echo labels print into the Airflow log.
INSERT_CMD = (
    f"docker exec -i {POSTGRES_CONTAINER} "
    f"psql -U postgres -d inventory -f - < {SQL_PATH}"
)

default_args = {
    "owner": "poc",
    "retries": 0,
}

with DAG(
    dag_id="cdc_data_generator",
    description="Insert 1 random customer + 1 random order per minute while enabled.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",         # every 1 minute
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # starts DISABLED; you enable it to begin
    tags=["cdc", "generator", "demo"],
) as dag:

    insert_random_records = BashOperator(
        task_id="insert_random_records",
        bash_command=INSERT_CMD,
    )
