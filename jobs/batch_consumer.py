"""
Airflow-orchestrated BATCH CDC consumer (Kafka -> Iceberg), engine-pluggable.

One run = one bounded pass:
  for each table in batch_config.json:
    1. resolve the Kafka offset RANGE to process:
         start = this consumer group's last committed offset per partition
                 (or earliest/latest if the group is brand new)
         end   = current end offset per partition, capped by max_records_per_run
    2. hand (start, end) to the configured engine's writer, which does the
       bounded read + dedup + MERGE INTO Iceberg
    3. ONLY if the write succeeds, COMMIT `end` back to the consumer group
Then exit. Airflow re-triggers on a schedule (e.g. every 5 min).

Offset management (Option A): offsets live in Kafka, tied to a consumer group,
committed only AFTER a successful write. A failure before commit means the next
run re-reads the same range (at-least-once). MERGE idempotency dedups, so the
Iceberg result is effectively exactly-once.

Engine selection: each table's `engine` ("spark" | "polars") picks the writer.
Spark is implemented; polars is reserved (writers/polars_writer.py).

Writes land in a dedicated namespace (default lakehouse.cdc_batch) so this job
never collides with the streaming job's lakehouse.cdc_v2 tables.

Usage (inside the spark container, so the Spark engine can spark-submit):
    spark-submit --master spark://spark-master:7077 --conf spark.cores.max=1 \
        /opt/spark/jobs/batch_consumer.py [path/to/batch_config.json]
"""

import json
import os
import sys

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

KAFKA_BOOTSTRAP = "kafka:9092"


def load_config(path):
    with open(path) as f:
        cfg = json.load(f)
    defaults = cfg.get("defaults", {})
    tables = cfg.get("tables", {})
    if not tables:
        raise ValueError(f"No 'tables' defined in config: {path}")
    return defaults, tables


def _setting(cfg, defaults, name, fallback=None):
    return cfg.get(name, defaults.get(name, fallback))


def resolve_offset_range(topic, group_id, max_records, starting_if_new):
    """
    Return (start_map, end_map, has_data) as Spark-Kafka offset dicts for one topic.

      start_map = {topic: {partition: start_offset}}   (inclusive)
      end_map   = {topic: {partition: end_offset}}      (exclusive)

    start = the group's committed offset per partition; if the group has never
    committed for a partition, use the partition's earliest/latest per config.
    end   = the partition's current end, but not more than `max_records` beyond
    start (so one run processes a bounded chunk; the next run picks up the rest).

    A short-lived KafkaConsumer (no auto-commit) is used purely to look up
    committed/beginning/end offsets. We do NOT commit here — commit happens only
    after the write succeeds (see commit_offsets).
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=starting_if_new,  # used only when no committed offset
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {}, {}, False
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]

        beginning = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)

        start_map = {}
        end_map = {}
        total = 0
        for tp in tps:
            committed = consumer.committed(tp)
            if committed is None:
                start = beginning[tp] if starting_if_new == "earliest" else end[tp]
            else:
                start = committed
            # never start before the earliest retained offset
            start = max(start, beginning[tp])

            partition_end = end[tp]
            # cap this partition's slice so the whole run stays bounded
            remaining = max_records - total
            if remaining <= 0:
                capped_end = start  # budget exhausted; take nothing more this run
            else:
                capped_end = min(partition_end, start + remaining)
            total += (capped_end - start)

            start_map[str(tp.partition)] = int(start)
            end_map[str(tp.partition)] = int(capped_end)

        has_data = any(end_map[p] > start_map[p] for p in start_map)
        return {topic: start_map}, {topic: end_map}, has_data
    finally:
        consumer.close()


def commit_offsets(topic, group_id, end_map):
    """
    Commit the processed end offsets to the consumer group AFTER a successful
    write. end_map = {topic: {partition: end_offset}}. For Kafka a committed
    offset means "next read starts here", which is exactly our exclusive end.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        enable_auto_commit=False,
    )
    try:
        offsets = {
            TopicPartition(topic, int(p)): OffsetAndMetadata(off, None)
            for p, off in end_map[topic].items()
        }
        consumer.commit(offsets=offsets)
        print(f">>> committed offsets for {topic}: {end_map[topic]}")
    finally:
        consumer.close()


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else \
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "batch_config.json")

    defaults, tables = load_config(config_path)

    # Group writers by engine so we create at most one Spark session per run.
    from writers import get_writer

    namespace = defaults.get("namespace", "lakehouse.cdc_batch")
    group_id = defaults.get("consumer_group", "cdc-batch-consumer")
    max_records = int(defaults.get("max_records_per_run", 100000))
    starting_if_new = defaults.get("starting_offsets_if_new", "earliest")

    print(f">>> batch run start | config={config_path}")
    print(f">>> namespace={namespace} group={group_id} max_records_per_run={max_records}")

    writers_cache = {}
    processed = 0
    failures = 0

    try:
        for name, cfg in tables.items():
            engine = _setting(cfg, defaults, "engine", "spark")
            topic = cfg["topic"]
            tbl_group = f"{group_id}-{name}"  # per-table group = independent offsets

            # 1. resolve the range to process this run
            start_map, end_map, has_data = resolve_offset_range(
                topic, tbl_group, max_records, starting_if_new
            )
            if not has_data:
                print(f">>> {name}: no new records; skipping")
                continue

            # 2. get (or build) the writer for this engine
            if engine not in writers_cache:
                writers_cache[engine] = get_writer(engine)
                if hasattr(writers_cache[engine], "ensure_namespace"):
                    writers_cache[engine].ensure_namespace(namespace)
            writer = writers_cache[engine]

            # 3. write, then commit offsets ONLY on success
            try:
                writer.write_batch(name, cfg, namespace, start_map, end_map)
                commit_offsets(topic, tbl_group, end_map)
                processed += 1
            except Exception as e:
                failures += 1
                print(f"!!! {name}: write failed, NOT committing offsets: {e}")
                # offsets not committed -> next run re-reads this range safely
                raise
    finally:
        for w in writers_cache.values():
            if hasattr(w, "stop"):
                w.stop()

    print(f">>> batch run done | tables_processed={processed} failures={failures}")


if __name__ == "__main__":
    main()
