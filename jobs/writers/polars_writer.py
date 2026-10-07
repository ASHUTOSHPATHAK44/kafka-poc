"""
Polars writer — RESERVED, not yet implemented.

The batch consumer supports an `engine` config value of "spark" or "polars".
The Spark path is implemented (writers/spark_writer.py). This Polars path is a
deliberate stub so the engine parameter is wired end-to-end now and a Polars +
PyIceberg implementation can drop in later without touching the consumer.

When implemented, this would:
  * build a Polars/Arrow DataFrame from the parsed Debezium records,
  * use PyIceberg against the REST catalog to upsert rows and delete by key,
  * (note: PyIceberg's delete/merge semantics differ from Spark SQL MERGE).
"""


class PolarsWriter:
    def __init__(self):
        raise NotImplementedError(
            "The Polars writer is not implemented yet. Set engine='spark' in "
            "batch_config.json. This stub keeps the 'engine' parameter wired so "
            "a Polars/PyIceberg backend can be added later without changing the "
            "batch consumer."
        )

    def write_batch(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError
