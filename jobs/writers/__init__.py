"""
Pluggable writer backends for the batch consumer.

The batch consumer resolves which writer to use from each table's `engine`
config value and calls writer.write_batch(...). Adding a new engine = add a
module here and register it in get_writer().
"""

from writers.spark_writer import SparkWriter

try:
    from writers.polars_writer import PolarsWriter
except Exception:  # pragma: no cover - optional dependency / not implemented yet
    PolarsWriter = None


def get_writer(engine: str):
    """Return a writer instance for the configured engine string."""
    engine = (engine or "spark").lower()
    if engine == "spark":
        return SparkWriter()
    if engine == "polars":
        if PolarsWriter is None:
            raise NotImplementedError(
                "engine 'polars' is configured but the Polars writer is not "
                "available/implemented yet. Use engine 'spark' for now."
            )
        return PolarsWriter()
    raise ValueError(f"Unknown engine '{engine}'. Supported: spark (polars reserved).")
