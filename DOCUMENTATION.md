# CDC Lakehouse POC — End-to-End Documentation

A local, fully-Dockerized Change Data Capture (CDC) pipeline that moves row-level
changes from PostgreSQL into Apache Iceberg tables stored on MinIO (S3), using
Debezium + Kafka as the transport.

It ships **two independent consumers** that read the same Kafka CDC topics:
- a **streaming** consumer (Spark Structured Streaming) — near-real-time, into
  `lakehouse.cdc_v2`, and
- a **batch** consumer (Spark batch, Airflow-orchestrated every 5 min, manual Kafka
  offset management, pluggable `engine`) — into `lakehouse.cdc_batch`.

An **Airflow data-generator DAG** can continuously insert random rows (toggle on/off)
to drive the pipeline. Everything runs inside Docker — no Python, Java, or Spark is
required on the host.

---

## 1. What this project does (in one picture)

```
 ┌────────────┐  logical   ┌────────────┐  Kafka    ┌────────────┐
 │ PostgreSQL │  decoding  │  Debezium  │  topics   │   Kafka    │
 │  (source   │───────────▶│  (Kafka    │──────────▶│  (broker,  │
 │   tables)  │  of WAL    │  Connect)  │ CDC events│   KRaft)   │
 └────────────┘            └────────────┘           └─────┬──────┘
       ▲                                                  │ same topics, two consumers
       │ inserts                      ┌───────────────────┴───────────────────┐
       │                              ▼                                        ▼
 ┌───────────────┐          ┌──────────────────────┐              ┌──────────────────────┐
 │ Airflow DAG   │          │ STREAMING consumer    │              │ BATCH consumer        │
 │ data-generator│          │ kafka_to_iceberg_v2   │              │ batch_consumer        │
 │ (1 row/min    │          │ Spark Structured      │              │ Spark batch read,     │
 │  while on)    │          │ Streaming (checkpoint)│              │ Airflow every 5 min,  │
 └───────────────┘          │ -> lakehouse.cdc_v2   │              │ Kafka offset commit,  │
                            └───────────┬──────────┘              │ engine=spark|polars   │
                                        │                         │ -> lakehouse.cdc_batch│
                                        │        MERGE INTO       └───────────┬──────────┘
                                        └────────────┬────────────────────────┘
                                                     ▼
                              ┌──────────────┐  table  ┌────────────┐
                              │ Iceberg REST │◀───────▶│   MinIO    │
                              │   Catalog    │  files  │  (S3 data  │
                              │ (metadata in │         │ + metadata │
                              │  Postgres)   │         │   files)   │
                              └──────────────┘         └────────────┘

 Observability UIs:  Kafka UI (8088) · MinIO Console (9001) ·
   Spark Master/Worker/History (8080/8081/18080) · Airflow (8085)
```

**The story:** a row changes in Postgres → Debezium captures it from the write-ahead
log and publishes a JSON event to a Kafka topic → **both** consumers read that topic:
the streaming job `MERGE`s it near-instantly into `lakehouse.cdc_v2`, and the
Airflow-triggered batch job sweeps new records every 5 minutes into
`lakehouse.cdc_batch`. Iceberg data files live in MinIO; the catalog metadata (which
tables exist, where their files are) lives in a Postgres database. Each Iceberg table
ends up a live mirror of the Postgres table.

---

## 2. Core concepts (glossary)

If you are new to this stack, read this section first — the rest of the doc assumes these.

### Kafka
A distributed, durable **log** system for streaming data. Producers append messages;
consumers read them. Unlike a traditional queue, **reading does not delete** messages —
they stay until a retention period expires, so many independent consumers can read (and
re-read) the same data. This "durable replayable log" is what makes CDC reliable.

### Broker
A single Kafka server process. Our setup runs **one broker** (`apache/kafka`) in
**KRaft mode** (Kafka Raft) — meaning it manages its own metadata and needs **no
ZooKeeper** (older Kafka required a separate ZooKeeper cluster).

### Topic
A named stream of messages, e.g. `poc.inventory.customers`. Think of it as a category /
feed. Debezium publishes one topic per source table. Topic names here follow Debezium's
rule: `<topic.prefix>.<schema>.<table>` → `poc` + `inventory` + `customers`.

### Partition
A topic is split into one or more **partitions**, each an independent append-only log.
Partitions are Kafka's unit of **parallelism** and **ordering**:
- Ordering is guaranteed **only within a partition**, not across a whole topic.
- A message's **key** decides its partition (same key → same partition → ordered).
  Debezium keys each event by the row's **primary key**, so all changes for one row
  stay in order.
- More partitions = more consumers can read in parallel.
- This POC uses **1 partition per topic** (see `KAFKA_NUM_PARTITIONS: 1`): perfect
  global ordering, no parallelism needed for low volume.

### Offset
A sequential position number **within a partition** (0, 1, 2, …). It is a consumer's
"bookmark" — where it has read up to. Offsets are **per-partition**. Spark stores its
offsets in its **checkpoint**, not in Kafka, which is how it resumes exactly where it
left off after a restart or failure.

### Producer / Consumer
- **Producer** = writes to Kafka. Here: **Debezium** (running on Kafka Connect).
- **Consumer** = reads from Kafka. Here: **Spark Structured Streaming**.

### Kafka Connect
A framework for moving data between external systems and Kafka via **connectors**.
Debezium is a set of connectors that runs *on* Kafka Connect. It exposes a **REST API**
(port 8083) used to register/manage connectors.

### CDC (Change Data Capture)
Capturing every insert/update/delete from a database as a stream of events. Debezium
does this by reading Postgres's **write-ahead log (WAL)** via **logical decoding** — so
it captures changes without polling the tables.

### Debezium event envelope
Each CDC message is a JSON envelope, not just a row:
- `before` — row state before the change (present for updates/deletes)
- `after` — row state after the change (present for inserts/updates)
- `op` — operation: `c` (create/insert), `u` (update), `d` (delete), `r` (snapshot read)
- `ts_ms` — event timestamp

### Apache Iceberg
An open **table format** for large analytic datasets on object storage. It adds
database-like features on top of files in S3/MinIO: schemas, `MERGE`/upsert/delete,
snapshots, and time travel. Data + metadata are stored as files (Parquet + manifests);
a **catalog** tracks which tables exist and where their current metadata file is.

### Iceberg REST Catalog
A service (port 8181) that answers "where does table X live?" Spark asks it, and it
returns a pointer to the table's metadata files in MinIO. Our catalog stores its own
bookkeeping in a **PostgreSQL** database (`iceberg_catalog`) so it handles concurrent
writers correctly and survives restarts.

### MinIO
An S3-compatible object store. It plays the role of the "data lake" — the actual
Iceberg Parquet data files and metadata files live in the `warehouse` bucket.

### Spark / Structured Streaming
Apache Spark is a distributed compute engine. **Structured Streaming** processes a
stream as a series of small **micro-batches** (not record-by-record). Each batch reads a
bounded range of Kafka offsets, processes them, and commits — giving exactly-once-style
guarantees when paired with an idempotent sink like `MERGE`.

### `MERGE INTO` (upsert + delete)
A single SQL statement that reconciles incoming changes against a target table:
insert new keys, update existing keys, delete removed keys. This is what keeps the
Iceberg table a faithful mirror of the Postgres table. It is **idempotent** on the key,
so replaying a batch after a failure cannot create duplicates.

### Checkpoint
A directory where Structured Streaming records, per query: which offsets a batch covered
(`offsets/`) and whether that batch finished (`commits/`). The "done" marker is written
**after** the data is safely written, so a crash mid-write causes the batch to replay
from the same offsets — no data loss, no duplicates.

### Trigger / `maxOffsetsPerTrigger`
- **Trigger** = how often a micro-batch fires. Default = back-to-back (as fast as
  possible). `processingTime="30 seconds"` = one batch every 30s.
- **`maxOffsetsPerTrigger`** = max records pulled per batch (batch-size cap). Bounds
  memory and limits how much replays on failure. `null` = unlimited.

### Consumer group (batch offset management)
A named identity Kafka uses to **remember how far a consumer has read**, per partition,
in its internal `__consumer_offsets`. The batch consumer uses one group per table and
commits the new offset **only after** a successful Iceberg write. If a run fails before
commit, the next run re-reads the same range (at-least-once) and `MERGE` idempotency
prevents duplicates. This is the batch equivalent of the streaming checkpoint. (Note:
`startingOffsets=earliest` means earliest *still retained* — see Section 15 on Kafka
retention.)

### Airflow
A workflow orchestrator. A **DAG** (directed acyclic graph) defines scheduled tasks.
Here it runs the batch consumer every 5 minutes and (optionally) generates demo data.
**Enabling** (unpausing) a DAG lets it run on its schedule; **disabling** (pausing) it
stops new runs — the on/off switch for both the batch job and the data generator.

---

## 3. Project structure

```
kafka-poc/
├── .env                          # Pinned versions + credentials for all services
├── docker-compose.yml            # The whole stack: all services, networks, volumes
├── DOCUMENTATION.md              # This file
│
├── conf/
│   └── spark-defaults.conf       # Spark's Iceberg catalog + MinIO/S3 wiring
│                                 #   (mounted read-only into Spark containers)
├── connectors/
│   └── postgres-inventory.json   # Debezium connector definition (registered via REST)
│
├── docker/
│   ├── postgres/
│   │   └── init/01_init.sql       # Source schema + seed data + publication;
│   │                              #   also creates the iceberg_catalog DB (fresh volume)
│   ├── spark/
│   │   └── Dockerfile             # Spark image + Iceberg & Kafka JARs + kafka-python
│   └── iceberg-rest/
│       └── Dockerfile             # Iceberg REST fixture + PostgreSQL JDBC driver added
│
├── airflow/
│   └── dags/
│       ├── cdc_batch_dag.py        # Triggers batch_consumer every 5 min
│       └── data_generator_dag.py   # Inserts 1 random customer+order/min while enabled
│
├── scripts/
│   └── generate_data.sql           # FK-safe random insert used by the generator DAG
│
└── jobs/                          # Jobs (mounted read-only at /opt/spark/jobs)
    ├── cdc_to_iceberg.py          # v1: original hard-coded 2-table streaming job
    ├── kafka_to_iceberg_v2.py     # v2: generic config-driven STREAMING engine
    ├── tables.json                # v2 streaming table catalog (topics, keys, columns)
    ├── batch_consumer.py          # BATCH consumer: offset-managed, engine-pluggable
    ├── batch_config.json          # batch table catalog (adds engine + cdc_batch namespace)
    ├── writers/
    │   ├── __init__.py            # get_writer(engine) factory (spark | polars)
    │   ├── spark_writer.py        # Spark batch read + MERGE writer (implemented)
    │   └── polars_writer.py       # reserved stub (not implemented yet)
    ├── test_iceberg.py            # Iceberg smoke test (write/read round-trip to MinIO)
    └── .gitkeep
```

### File-by-file

| File | Purpose |
|------|---------|
| `.env` | Single source of pinned versions (Spark 3.5.9, Iceberg 1.10.1, Debezium 3.6.3, Kafka 3.9.2, etc.) and all credentials. Compose reads `${VAR}` from here. |
| `docker-compose.yml` | Declares every service, their config, health checks, ports (all bound to `127.0.0.1`), the private `poc` network, and named volumes for persistence. |
| `conf/spark-defaults.conf` | Configures the Spark Iceberg catalog named `lakehouse` (REST type, points at `iceberg-rest:8181`), S3FileIO → MinIO, path-style access. Mounted, so edits need no image rebuild. |
| `connectors/postgres-inventory.json` | The Debezium connector config: which DB/tables to capture, `pgoutput` plugin, `dbz_publication`, JSON converters, topic auto-creation. Registered via the Connect REST API. |
| `docker/postgres/init/01_init.sql` | Creates `inventory.customers` / `inventory.orders`, seeds rows, sets `REPLICA IDENTITY FULL`, creates `dbz_publication`, and creates the `iceberg_catalog` DB/role. Runs only on a fresh Postgres volume. |
| `docker/spark/Dockerfile` | Builds a Spark image (Java 17 + Python in-image) with the Iceberg Spark runtime, iceberg-aws-bundle, and Kafka connector JARs pre-installed, plus `kafka-python` (for the batch consumer's offset management). |
| `docker/iceberg-rest/Dockerfile` | Extends the Iceberg REST fixture image and adds the PostgreSQL JDBC driver (with world-readable perms) so the catalog can use Postgres as its backend. |
| `jobs/kafka_to_iceberg_v2.py` | STREAMING engine — reads `tables.json`, and for each table streams Kafka → parses Debezium JSON → `MERGE INTO` Iceberg. Namespace `lakehouse.cdc_v2`. |
| `jobs/tables.json` | Streaming per-table config: `topic`, `key`, `columns` (name+type). Columns drive both the parse schema and the DDL. Add a table = one entry here. |
| `jobs/batch_consumer.py` | BATCH consumer — resolves each table's Kafka offset range (consumer group), delegates to the engine writer, commits offsets only after a successful write. Airflow-triggered. |
| `jobs/batch_config.json` | Batch per-table config — like `tables.json` but adds `engine` (spark/polars), `namespace: lakehouse.cdc_batch`, `consumer_group`, `max_records_per_run`. |
| `jobs/writers/__init__.py` | `get_writer(engine)` factory routing to Spark or Polars. |
| `jobs/writers/spark_writer.py` | Spark batch read of the offset range + dedup + `MERGE INTO`; prints the records written each run. |
| `jobs/writers/polars_writer.py` | Reserved stub — raises "not implemented" so the `engine` param is wired for a future Polars/PyIceberg backend. |
| `jobs/cdc_to_iceberg.py` | The original v1 streaming job (kept working, hard-coded tables, namespace `lakehouse.cdc`). |
| `jobs/test_iceberg.py` | Standalone smoke test that creates a tiny Iceberg table and verifies data lands in MinIO. |
| `airflow/dags/cdc_batch_dag.py` | DAG — every 5 min, `docker exec`s `spark-submit batch_consumer.py` into the spark-client container. |
| `airflow/dags/data_generator_dag.py` | DAG — every 1 min while enabled, inserts 1 random customer + 1 random order (toggle on/off to start/stop). |
| `scripts/generate_data.sql` | FK-safe random insert (customer + order) with `RETURNING`, used by the generator DAG; prints inserted rows into the Airflow log. |

---

## 4. Services (from docker-compose.yml)

| # | Service | Image | Role | Host port(s) |
|---|---------|-------|------|--------------|
| 1 | `postgres` | `postgres:16.10` | Source DB (WAL logical decoding on) + catalog metadata DB | 5432 |
| 2 | `kafka` | `apache/kafka:3.9.2` | Single-node KRaft broker (the durable log) | 29092 |
| 3 | `kafka-ui` | `kafbat/kafka-ui:v1.5.0` | Web UI for topics/messages/connectors | 8088 |
| 4 | `connect` | `quay.io/debezium/connect:3.6.3.Final` | Producer: Debezium on Kafka Connect | 8083 |
| 5 | `minio` | `quay.io/minio/minio:RELEASE.2025-04-22...` | S3 object store (the data lake) | 9000 (S3), 9001 (console) |
| 6 | `minio-init` | `quay.io/minio/mc:RELEASE.2025-08-13...` | One-shot: creates the `warehouse` bucket, then exits | — |
| 7 | `iceberg-rest` | custom (fixture + PG driver) | Iceberg REST catalog, metadata in Postgres | 8181 |
| 8 | `spark-master` | custom Spark image | Standalone cluster master (scheduler) | 8080 (UI), 7077 (submit) |
| 9 | `spark-worker` | custom Spark image | Executes tasks (2 cores, 2 GB) | 8081 (UI) |
| 10 | `spark-history` | custom Spark image | Spark UI for completed apps | 18080 |
| 11 | `spark-client` | custom Spark image | Long-lived container to run spark-submit / spark-sql | 4040 (driver UI) |
| 12 | `airflow-db` | `postgres:16.10` | Airflow's own metadata DB (isolated from the CDC/catalog Postgres) | — |
| 13 | `airflow` | `apache/airflow:2.10.5` | Orchestrator (standalone: scheduler + webserver); triggers the batch + generator DAGs | 8085 (UI) |

> Security note: this stack is intentionally open (PLAINTEXT Kafka, no auth on Connect /
> catalog / Spark, default MinIO + Airflow creds). All ports bind to `127.0.0.1` only.
> The Airflow container mounts the host Docker socket so its DAGs can `docker exec` into
> other containers — convenient for a local POC, but a privilege to be aware of. Do not
> expose beyond localhost or reuse this config outside a local POC.

### Version compatibility (why these versions)
- **Spark 3.5.9** → Scala 2.12, Java 17, Python in-image.
- **Iceberg 1.10.1** → has both a matching `iceberg-spark-runtime-3.5_2.12` **and** a
  matching `apache/iceberg-rest-fixture:1.10.1` image, so engine and catalog align.
- **kafka-clients 3.4.1** → the version Spark 3.5.9's `spark-sql-kafka-0-10` pins.
- **Kafka broker 3.9.2 (KRaft)** → interops with the 3.4.1 client and Debezium.
- **Debezium 3.6.3.Final** → supports Postgres 16 via `pgoutput`.
- **MinIO `RELEASE.2025-04-22`** → last community release with a usable web console.
- Spark writes to MinIO via Iceberg's own **S3FileIO** + `iceberg-aws-bundle`, avoiding
  the usual hadoop-aws/AWS-SDK version-matching pain.

---

## 5. End-to-end data flow

### 5.1 Producer side (Postgres → Kafka)

```
1. A row changes in inventory.customers (INSERT / UPDATE / DELETE), committed to the WAL.
2. Debezium (on Kafka Connect) reads the WAL via logical decoding (pgoutput plugin,
   dbz_publication). REPLICA IDENTITY FULL ensures the full `before` image is captured.
3. Debezium builds a JSON envelope { before, after, op, ts_ms } keyed by primary key.
4. It publishes to topic poc.inventory.customers (auto-created, 1 partition).
```

### 5.2 Consumer side (Kafka → Iceberg → MinIO)

```
5. Spark Structured Streaming (kafka_to_iceberg_v2.py) reads the topic as a stream.
6. Each micro-batch:
     a. CAST Kafka value bytes → JSON string
     b. from_json(...) parse using the Debezium envelope schema (built from tables.json)
     c. coalesce(after, before) → the row; carry op + ts_ms
     d. dedup within the batch: keep the latest event per key (row_number by ts_ms desc)
     e. MERGE INTO the Iceberg table:
          op='d'  + match      → DELETE
          op<>'d' + match      → UPDATE (upsert)
          op<>'d' + no match   → INSERT
7. Iceberg writes Parquet data + metadata files to MinIO (s3://warehouse/cdc_v2/<table>/).
8. The Iceberg REST catalog records the new table metadata pointer in the
   iceberg_catalog Postgres DB.
9. Spark commits the batch's offsets to its checkpoint (/opt/spark/checkpoints_v2/<table>).
```

### 5.3 Why it's reliable
- **Ordering:** 1 partition → globally ordered; Debezium keys by PK so per-row order is
  preserved even if partitions increase later.
- **Exactly-once effect:** checkpoint records "batch done" only *after* the write
  succeeds; on crash the batch replays from the same offsets, and `MERGE` idempotency
  prevents duplicates.
- **Concurrency:** the catalog on Postgres serializes concurrent commits from the two
  streaming queries (this replaced SQLite, which threw `SQLITE_BUSY` locks).

---

## 6. The streaming engine (kafka_to_iceberg_v2.py) at a glance

Config-driven: `tables.json` declares each table once; the engine loops over them.

| Function | Responsibility |
|----------|----------------|
| `load_config(path)` | Read `tables.json` → `(defaults, tables)`. |
| `build_after_schema(columns)` | Turn the `columns` list into a Spark `StructType` (used to parse the JSON row). |
| `build_ddl(columns)` | Turn the same `columns` list into `CREATE TABLE` column SQL. (Same source → schema & DDL can't drift.) |
| `debezium_envelope_schema(after_schema)` | Wrap the row schema in the `{before, after, op, ts_ms}` envelope. |
| `start_table_stream(...)` | For one table: create the Iceberg table, open the Kafka `readStream`, parse the envelope, and start a `writeStream` with `foreachBatch`. |
| `make_foreach_batch(...)` | **Factory** that returns an `upsert(batch_df, batch_id)` callback closing over the table's name/key/columns. Spark calls this callback per micro-batch. |
| `main()` | Load config, create Spark session + namespace, start one streaming query per table, `awaitAnyTermination()`. |

### tables.json shape
```json
{
  "defaults": { "starting_offsets": "earliest",
                "max_offsets_per_trigger": null,
                "processing_time": null },
  "tables": {
    "customers": {
      "topic": "poc.inventory.customers",
      "key": "id",
      "columns": [ ["id","BIGINT"], ["first_name","STRING"], ... ]
    }
  }
}
```
Column `type` maps **Debezium's JSON output**, not Postgres types:
`timestamptz → STRING` (ISO string), `numeric → DOUBLE`, `int → INT/BIGINT`, etc.

### Adding a new table (the whole change set)
1. **Postgres:** create the table + `ALTER TABLE ... REPLICA IDENTITY FULL`.
2. **Postgres:** `ALTER PUBLICATION dbz_publication ADD TABLE inventory.<t>;`
3. **Debezium:** add `inventory.<t>` to `table.include.list` and update the connector.
4. **Engine:** add one entry to `tables.json` (and/or `batch_config.json` for the batch job).
5. Restart the streaming job. (Kafka topic, Iceberg table, MinIO files, catalog entry are
   all created automatically.) See Section 10 for the full worked example.

---

## 7. The batch pipeline (batch_consumer.py)

A second consumer that reads the **same** Kafka CDC topics but in **bounded batches**
instead of a continuous stream, orchestrated by Airflow. Writes to a separate namespace
`lakehouse.cdc_batch`, so it never collides with the streaming job's `lakehouse.cdc_v2`.

### How one run works
```
For each table in batch_config.json:
  1. Resolve the Kafka offset RANGE to process:
       start = this table's consumer group's last committed offset
               (or earliest/latest if the group is new)
       end   = current end offset, capped by max_records_per_run
  2. Spark BATCH read of exactly [start, end)  (spark.read, NOT readStream)
  3. Parse Debezium envelope -> dedup latest-per-key -> MERGE INTO lakehouse.cdc_batch.<t>
  4. COMMIT `end` to the consumer group  — ONLY if the write succeeded
Then the job EXITS. Airflow re-triggers it on the next schedule tick.
```

### Offset management (manual, Option A — Kafka consumer group)
- Offsets live in Kafka, tied to a per-table consumer group (`cdc-batch-consumer-<table>`).
- Committed **only after** a successful Iceberg write (step 4).
- If a run fails before commit, offsets don't advance → the next run **re-reads** the same
  range (at-least-once) → `MERGE` idempotency means no duplicates. This is the batch
  equivalent of the streaming checkpoint, and satisfies "don't lose data on mid-job
  failure."

### The `engine` parameter (spark | polars)
Each table declares `engine` in `batch_config.json`. `batch_consumer.py` resolves it via
`writers/get_writer(engine)`:
- `spark` — implemented (`writers/spark_writer.py`): Spark batch read + `MERGE INTO`.
- `polars` — **reserved** (`writers/polars_writer.py` raises "not implemented"). The
  parameter is wired end-to-end so a Polars/PyIceberg writer can drop in later without
  touching the consumer.

### batch_config.json shape
```json
{
  "defaults": {
    "engine": "spark",
    "namespace": "lakehouse.cdc_batch",
    "consumer_group": "cdc-batch-consumer",
    "max_records_per_run": 100000,
    "starting_offsets_if_new": "earliest"
  },
  "tables": {
    "customers": { "topic": "poc.inventory.customers", "key": "id",
                   "engine": "spark",
                   "columns": [ ["id","BIGINT"], ... ] }
  }
}
```

### Key functions (batch_consumer.py)
| Function | Responsibility |
|----------|----------------|
| `load_config(path)` | Read `batch_config.json` → `(defaults, tables)`. |
| `resolve_offset_range(...)` | Use a `kafka-python` consumer to look up the group's committed offset and the topic's current end; return the `[start, end)` range (capped by `max_records_per_run`). Does NOT commit. |
| `commit_offsets(...)` | Commit `end` to the consumer group — called only after a successful write. |
| `main()` | Loop tables → resolve range → write via the engine writer → commit; exits. |
| `writers.spark_writer.SparkWriter.write_batch(...)` | Spark batch read of the range + dedup + `MERGE INTO`; prints the records written this run. |

### Run it manually (without waiting for Airflow)
```powershell
docker compose exec spark-client /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --conf spark.cores.max=1 `
  /opt/spark/jobs/batch_consumer.py

# verify
docker compose exec spark-client /opt/spark/bin/spark-sql `
  --master spark://spark-master:7077 --conf spark.cores.max=1 `
  -e "SELECT * FROM lakehouse.cdc_batch.customers ORDER BY id"
```

### Inspect / reset batch offsets (the batch analog of a checkpoint)
```powershell
# describe a table's batch consumer group (CURRENT-OFFSET / LOG-END-OFFSET / LAG)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 --describe --group cdc-batch-consumer-customers

# reset to re-read from earliest (stop producing/consuming first; safe — MERGE is idempotent)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 --group cdc-batch-consumer-customers `
  --topic poc.inventory.customers --reset-offsets --to-earliest --execute
```

---

## 8. Airflow orchestration & the data generator

Airflow runs in **standalone** mode (scheduler + webserver in one container), with its
own metadata Postgres (`airflow-db`). DAGs live in `airflow/dags/` (mounted read-only).
The Airflow container mounts the host Docker socket so a DAG can `docker exec` into other
containers — that's how it triggers Spark and Postgres without running them itself.

UI: http://localhost:8085 (login `admin` / `admin`).

### DAG 1 — `cdc_batch_to_iceberg` (the batch consumer)
- **Schedule:** every 5 minutes (`*/5 * * * *`), `catchup=False`, `max_active_runs=1`.
- **Action:** `docker exec poc-spark-client spark-submit ... batch_consumer.py`.
- **Log readability:** the batch run prints, per table, the exact records written this
  run (a `.show()` of the deduped change set) and a MERGE-complete line.
- **Control:** enable to run on schedule; pause to stop.

### DAG 2 — `cdc_data_generator` (demo data source)
- **Schedule:** every 1 minute (`* * * * *`), created **paused** by default.
- **Action:** `docker exec -i poc-postgres psql ... -f -` fed `scripts/generate_data.sql`,
  which inserts one random customer + one random order (FK-safe) and uses `RETURNING` so
  the **inserted rows print into the Airflow task log**.
- **Control (what you asked for):** **enable** → it inserts 1 row-pair per minute;
  **disable (pause)** → insertion stops; **re-enable** → it resumes. No overlap/backfill.

### Typical demo loop
```
1. Enable cdc_data_generator   -> rows start flowing into Postgres every minute
2. Streaming job (cdc_v2)      -> reflects each row within seconds
3. cdc_batch_to_iceberg (5 min)-> sweeps the new rows into cdc_batch in bounded batches
4. Disable cdc_data_generator  -> generation stops
```
This also makes the **streaming-vs-batch latency** visible: `cdc_v2` updates in seconds,
`cdc_batch` lags up to 5 minutes (its schedule).

---

## 9. How to run

All commands run from `c:\kafka-poc`. Nothing is needed on the host except Docker.

```powershell
# 1. Build + start the whole stack
docker compose up -d --build

# 2. Check status (minio-init should show Exited(0); everything else running/healthy)
docker compose ps

# 3. Register the Debezium connector (producer side)
curl.exe -s -X POST -H "Content-Type: application/json" `
  --data "@connectors/postgres-inventory.json" http://localhost:8083/connectors

# 4. Verify connector + topics
curl.exe -s http://localhost:8083/connectors/inventory-connector/status
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# 5. Run the streaming job (long-running; cap at 1 core so queries can share the worker)
docker compose exec spark-client /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --conf spark.cores.max=1 `
  /opt/spark/jobs/kafka_to_iceberg_v2.py

# 6. In a SECOND terminal — verify the Iceberg tables
docker compose exec spark-client /opt/spark/bin/spark-sql `
  --master spark://spark-master:7077 --conf spark.cores.max=1 `
  -e "SELECT * FROM lakehouse.cdc_v2.customers ORDER BY id;"

# 7. Live CDC test — change data in Postgres and watch it flow through
docker compose exec postgres psql -U postgres -d inventory
#   INSERT/UPDATE/DELETE on inventory.customers, then re-run step 6.

# 8. (Batch path) open Airflow at http://localhost:8085 (admin/admin) and:
#     - enable  cdc_data_generator   -> inserts 1 random customer+order per minute
#     - enable  cdc_batch_to_iceberg -> sweeps new records into lakehouse.cdc_batch every 5 min
#    Or trigger the batch job manually (see Section 7).
```

Lifecycle:
```powershell
docker compose stop     # pause (keeps data + images)
docker compose start    # resume
docker compose down     # remove containers (keeps volumes)
docker compose down -v  # remove containers AND data volumes
```

### UIs
| UI | URL | Notes |
|----|-----|-------|
| Kafka UI | http://localhost:8088 | topics, messages, connector status |
| MinIO Console | http://localhost:9001 | login `minioadmin` / `minioadmin`; browse `warehouse` |
| Spark Master | http://localhost:8080 | workers, running/waiting apps, cores |
| Spark Worker | http://localhost:8081 | executor detail |
| Spark History | http://localhost:18080 | completed applications |
| Spark Driver | http://localhost:4040 | live streaming queries (while a job runs) |
| Kafka Connect API | http://localhost:8083 | connector REST management |
| Iceberg REST | http://localhost:8181 | `/v1/config`, `/v1/namespaces` |
| Airflow | http://localhost:8085 | login `admin` / `admin`; enable/pause DAGs, view task logs |

---

## 10. Alternatives (what each layer could be swapped for)

This POC made specific choices; here are the realistic alternatives if requirements change.

### Source database
- **Used:** PostgreSQL (logical decoding / `pgoutput`).
- **Alternatives:** MySQL (binlog), SQL Server (CDC tables), Oracle (LogMiner),
  MongoDB (change streams) — all supported by Debezium connectors.

### CDC / producer
- **Used:** Debezium on Kafka Connect.
- **Alternatives:**
  - **Kafka Connect JDBC source** — polling-based, simpler, but misses deletes and adds
    query load (not true log-based CDC).
  - **Airbyte / Fivetran / Meltano** — managed/ELT-style CDC.
  - **Flink CDC** — Debezium embedded in Flink, skipping Kafka for some designs.

### Streaming transport
- **Used:** Apache Kafka (single-node KRaft).
- **Alternatives:** Redpanda (Kafka-API-compatible, no JVM/ZooKeeper), AWS MSK / Confluent
  Cloud (managed Kafka), Apache Pulsar. For non-streaming, you could skip a bus entirely
  and do batch extracts — but you lose real-time.

### Serialization / schema
- **Used:** JSON with schemas disabled (simple, human-readable).
- **Alternatives:** **Avro + Schema Registry** or **Protobuf** — compact, schema-evolution
  support, and Spark can read the schema automatically instead of hand-declaring
  `columns` in `tables.json`. Standard for production.

### Processing / consumer
- **Used:** Spark Structured Streaming (standalone cluster).
- **Alternatives:**
  - **Apache Flink** — lower-latency, event-at-a-time streaming; strong CDC story.
  - **Kafka Streams / ksqlDB** — JVM-native stream processing without a separate cluster.
  - **Spark on Kubernetes/YARN** — instead of standalone, for real clusters with dynamic
    resource allocation.

### Table format (lakehouse)
- **Used:** Apache Iceberg.
- **Alternatives:** **Delta Lake** (tight Spark/Databricks integration), **Apache Hudi**
  (built for upserts/CDC, copy-on-write & merge-on-read). All three support ACID upserts
  on object storage.

### Iceberg catalog
- **Used:** REST catalog backed by PostgreSQL (JDBC).
- **Alternatives:**
  - **In-memory** (fixture default) — simplest, but metadata lost on restart; used for
    throwaway demos.
  - **Hive Metastore** — classic, heavier.
  - **AWS Glue** / **Nessie** (Git-like versioned catalog) / **Unity Catalog** — managed
    or advanced options.
  - SQLite JDBC — works single-process, but locks (`SQLITE_BUSY`) under concurrent
    writers; avoided here.

### Object storage
- **Used:** MinIO (local S3).
- **Alternatives:** AWS S3, GCS, Azure Blob/ADLS, Ceph. Because we use S3FileIO, moving to
  real S3 is mostly an endpoint + credentials change.

### Write strategy
- **Used:** `MERGE INTO` (upsert + delete) → table mirrors source (Option A).
- **Alternative:** **append-only** — store every change event as a new row (a full change
  history / SCD-style log) instead of current state. Simpler, but not a live mirror.

### Consumer parallelism
- **Used:** 1 partition, 1 core.
- **Alternative:** more partitions + more Spark cores/executors for throughput; add
  `maxOffsetsPerTrigger` and a `processingTime` trigger to control batch size and file
  counts at scale.

### Consumption mode (streaming vs batch)
- **Used:** both — a streaming consumer (`cdc_v2`, near-real-time) and a batch consumer
  (`cdc_batch`, Airflow every 5 min).
- **Alternatives:** streaming-only (lowest latency), batch-only (simplest ops, higher
  latency), or micro-batch tuned via `processingTime`. The batch `engine` param also
  reserves a non-Spark path (Polars/PyIceberg) for lighter workloads.

### Orchestration
- **Used:** Apache Airflow (standalone), triggering jobs via `docker exec`.
- **Alternatives:** cron/Windows Task Scheduler (trivial), Dagster or Prefect (modern
  Python orchestrators), Argo Workflows (Kubernetes). For triggering Spark specifically,
  the Airflow `SparkSubmitOperator`/`DockerOperator` are cleaner than shelling out in a
  larger deployment.

---

## 11. Known POC simplifications (not production-ready as-is)

- No authentication/TLS anywhere; default credentials (including Airflow admin/admin);
  single broker (no replication).
- 1 partition per topic (no consumer parallelism).
- JSON (no schema registry / schema evolution).
- Spark standalone with 2 cores (jobs capped at 1 core to share the worker).
- Catalog metadata persists in Postgres, but the source and catalog share one Postgres
  container (Airflow has its own separate `airflow-db`).
- Timestamps stored as ISO **strings** (matching Debezium output) rather than native
  `TIMESTAMP` columns.
- Airflow mounts the host Docker socket to `docker exec` into other containers — a known
  privilege, acceptable for a local POC only.
- Default Kafka retention (7 days) applies — see Section 15; data older than retention
  can't be replayed by a new/offline consumer.

These are deliberate trade-offs for a local learning/POC environment and map directly to
the alternatives in Section 10 when hardening for production.

---

## 12. Adding a new table to the CDC pipeline (worked example)

A new table must be "let through" at three gates — **Postgres publication**, **Debezium
include-list**, and the **v2 `tables.json`**. Everything downstream (Kafka topic, Iceberg
table, MinIO files, catalog entry) is then automatic.

Worked example: add a `products` table (`id`, `name`, `price`, `in_stock`, `updated_at`).

### Step 1 — Create the table in Postgres and set replica identity
```powershell
docker compose exec postgres psql -U postgres -d inventory
```
Then at the `inventory=#` prompt:
```sql
CREATE TABLE inventory.products (
    id          serial PRIMARY KEY,
    name        varchar(200) NOT NULL,
    price       numeric(12,2) NOT NULL,
    in_stock    boolean NOT NULL DEFAULT true,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL makes Debezium emit the full `before` image on
-- updates/deletes (needed so deletes carry the key for MERGE).
ALTER TABLE inventory.products REPLICA IDENTITY FULL;

-- Optionally seed a couple of rows so the initial snapshot has data.
INSERT INTO inventory.products (name, price) VALUES ('Widget', 9.99), ('Gadget', 19.50);
```

### Step 2 — Add the table to the Debezium publication
Still in psql:
```sql
ALTER PUBLICATION dbz_publication ADD TABLE inventory.products;
\q
```

### Step 3 — Add the table to the connector's include list
Edit `connectors/postgres-inventory.json` and extend `table.include.list`:
```json
"table.include.list": "inventory.customers,inventory.orders,inventory.products",
```
Then push the updated config to the running connector (PUT the `config` object to the
connector's `/config` endpoint). Simplest is to re-apply the whole connector config:
```powershell
# Update the existing connector's config in place:
curl.exe -s -X PUT -H "Content-Type: application/json" `
  --data "@connectors/postgres-inventory-config.json" `
  http://localhost:8083/connectors/inventory-connector/config
```
> Note: the PUT endpoint expects **only the inner `config` object**, not the full
> `{name, config}` wrapper. Either keep a `postgres-inventory-config.json` containing just
> the config object, or delete + recreate the connector with POST (see Section 11).

Verify the new topic appears:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
# expect: poc.inventory.products
```

### Step 4 — Add the table to tables.json (the only "code" change)
Add one entry under `"tables"`. Type each column as **Debezium emits it in JSON**
(timestamptz → STRING, numeric → DOUBLE, boolean → BOOLEAN, serial/int → INT/BIGINT):
```json
"products": {
  "topic": "poc.inventory.products",
  "key": "id",
  "columns": [
    ["id",         "BIGINT"],
    ["name",       "STRING"],
    ["price",      "DOUBLE"],
    ["in_stock",   "BOOLEAN"],
    ["updated_at", "STRING"]
  ]
}
```

### Step 5 — Restart the v2 streaming job
The engine reads `tables.json` at startup, so restart to pick up the new table:
```powershell
# Stop the running job in its terminal with Ctrl+C, then relaunch:
docker compose exec spark-client /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --conf spark.cores.max=1 `
  /opt/spark/jobs/kafka_to_iceberg_v2.py
```
On restart it creates `lakehouse.cdc_v2.products`, starts a third streaming query, and
gets a new checkpoint at `/opt/spark/checkpoints_v2/products`. The existing tables'
checkpoints are untouched, so `customers`/`orders` resume where they were.

### Step 6 — Verify
```powershell
docker compose exec spark-client /opt/spark/bin/spark-sql `
  --master spark://spark-master:7077 --conf spark.cores.max=1 `
  -e "SELECT * FROM lakehouse.cdc_v2.products ORDER BY id;"
```

### What is manual vs automatic

| Step | Manual? |
|------|---------|
| Create Postgres table + `REPLICA IDENTITY FULL` | Yes |
| `ALTER PUBLICATION ... ADD TABLE` | Yes |
| Add to connector `table.include.list` + push | Yes |
| Add entry to `tables.json` (streaming) and/or `batch_config.json` (batch) | Yes (the one code edit) |
| Restart the streaming job / next batch run | Yes |
| Kafka topic creation | Automatic |
| Iceberg table + MinIO files + catalog entry | Automatic (`CREATE TABLE IF NOT EXISTS`) |

> For the **batch** pipeline, add the same table (with `engine`) to `batch_config.json`;
> no restart needed — the next 5-min Airflow run picks it up (the batch job reads the
> config fresh each run).

---

## 13. Viewing logs (per service)

`docker compose logs <service>` shows a service's output. Service names come from the
compose file (Section 4): `postgres`, `kafka`, `kafka-ui`, `connect`, `minio`,
`minio-init`, `iceberg-rest`, `spark-master`, `spark-worker`, `spark-history`,
`spark-client`, `airflow-db`, `airflow`.

```powershell
# Last N lines of one service
docker compose logs iceberg-rest --tail 50
docker compose logs connect      --tail 50
docker compose logs kafka        --tail 50
docker compose logs spark-master --tail 50
docker compose logs spark-worker --tail 50

# Follow live (Ctrl+C to stop following; does NOT stop the container)
docker compose logs -f connect
docker compose logs -f iceberg-rest

# Since a time window / with timestamps
docker compose logs iceberg-rest --since 10m
docker compose logs kafka --timestamps --tail 100

# All services at once (useful at startup)
docker compose logs --tail 20

# Filter for errors across a service's whole log (PowerShell)
docker compose logs iceberg-rest 2>&1 | Select-String -Pattern "ERROR|Exception|Caused by" | Select-Object -Last 25
```

### Spark job logs specifically
The **streaming job** (`spark-submit`) prints its driver logs directly to the terminal
where you launched it (client mode). That includes the `>>> batch N: merged ...` lines
and any errors. For **executor** detail, use the Spark UIs:
- Live job: **Spark Master UI** http://localhost:8080 → click the running app → stdout/stderr per executor.
- Driver UI (while running): http://localhost:4040 → **Structured Streaming** tab shows each query's progress/inputRate.
- Finished apps: **Spark History** http://localhost:18080.

### Airflow / batch & generator logs
The batch job and generator run under Airflow, so their output lands in the **Airflow
task logs**:
- UI: http://localhost:8085 → DAG → a run → task → **Logs**.
  - `cdc_batch_to_iceberg` log shows the records written each run (a table of the deduped
    changes) and `MERGE complete` lines.
  - `cdc_data_generator` log shows the inserted customer + order rows (via `RETURNING`).
- Or from the shell:
  ```powershell
  docker compose logs airflow --tail 100          # scheduler/webserver + task output
  docker compose logs -f airflow                  # follow live
  ```

### Service status / health
```powershell
docker compose ps                 # all services, state, health
docker compose ps iceberg-rest    # one service
docker stats --no-stream          # live CPU/memory per container
```

---

## 14. Inspecting checkpoints (streaming) and offsets (batch)

Structured Streaming stores each query's progress under
`/opt/spark/checkpoints_v2/<table>` inside the Spark containers (mounted on the
`spark-checkpoints` named volume, shared by all Spark services). v1 uses
`/opt/spark/checkpoints/<table>`.

```powershell
# List all v2 checkpoint dirs (one per table/query)
docker compose exec spark-client ls -la /opt/spark/checkpoints_v2

# Inside one table's checkpoint: offsets/ (planned), commits/ (completed), sources/, state/
docker compose exec spark-client ls -la /opt/spark/checkpoints_v2/customers
docker compose exec spark-client ls -la /opt/spark/checkpoints_v2/customers/offsets
docker compose exec spark-client ls -la /opt/spark/checkpoints_v2/customers/commits

# See the latest planned offsets (the Kafka partition->offset the batch covers)
docker compose exec spark-client sh -c "cat /opt/spark/checkpoints_v2/customers/offsets/\$(ls /opt/spark/checkpoints_v2/customers/offsets | sort -n | tail -1)"

# See the latest completed commit (proves that batch's write finished)
docker compose exec spark-client sh -c "ls /opt/spark/checkpoints_v2/customers/commits | sort -n | tail -1"
```

How to read it:
- A file in **offsets/** but no matching file in **commits/** = that batch was planned
  but not confirmed done → on restart it **replays** from those offsets (no data loss;
  MERGE idempotency prevents duplicates).
- The offsets file contains the Kafka `topic -> {partition: offset}` map for that batch.

### Resetting a checkpoint (force a clean re-read from `earliest`)
Deleting a checkpoint makes that query forget its position; with
`starting_offsets: earliest` it reprocesses the whole topic (safe here — MERGE is
idempotent). Stop the job first (Ctrl+C), then:
```powershell
docker compose exec spark-client rm -rf /opt/spark/checkpoints_v2/customers
# then relaunch the job
```
> Only do this intentionally. With `starting_offsets: latest` a reset would **skip**
> unread data. Treat the checkpoint as the thing that guarantees no loss/duplication.

### Kafka Connect offsets (producer side, for completeness)
Debezium tracks how far it has read the WAL in the internal `_connect_offsets` topic and
its replication slot in Postgres — separate from Spark's checkpoints:
```powershell
# Debezium's replication slot in Postgres
docker compose exec postgres psql -U postgres -d inventory -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"

# Connect's internal offset topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic _connect_offsets --from-beginning --timeout-ms 5000
```

### Batch consumer offsets (Kafka consumer groups)
The batch job's position lives in Kafka, one group per table:
```powershell
# list batch consumer groups
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list

# describe one: CURRENT-OFFSET vs LOG-END-OFFSET vs LAG
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 --describe --group cdc-batch-consumer-customers
```
`LAG = 0` means the batch job has consumed everything currently in the topic.

---

## 15. Gotcha: Kafka retention vs `earliest`

`startingOffsets=earliest` (streaming) and `starting_offsets_if_new=earliest` (batch)
mean **earliest still retained**, not "earliest ever." Kafka deletes messages older than
the topic's retention (default **7 days**). Consequences we actually hit in this POC:

- A consumer that is new, or offline longer than the retention window, **cannot replay**
  data that has already aged out — those records are gone from Kafka. This is true for
  *any* consumer (streaming or batch), not a bug in the job.
- Symptom: a fresh `cdc_batch` table showed only recent rows because the seed rows
  (produced > 7 days earlier) had expired from the topic; the batch job correctly
  processed only what still existed (from the earliest retained offset onward).

What to do about it:
- **Re-seed from the database**, not from Kafka — trigger a fresh Debezium snapshot
  (e.g. recreate the connector, or use a snapshot signal) so current DB state is
  re-published to the topic, then the consumers pick it up.
- **Increase retention** for longer replay windows:
  ```powershell
  docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 `
    --entity-type topics --entity-name poc.inventory.customers `
    --alter --add-config retention.ms=2592000000   # 30 days
  ```
- Or use a **log-compacted** topic to keep the latest value per key indefinitely.

The lakehouse tables (`cdc_v2`, `cdc_batch`) are unaffected by Kafka retention once data
has been written — they persist in MinIO regardless of what Kafka later deletes.
```
