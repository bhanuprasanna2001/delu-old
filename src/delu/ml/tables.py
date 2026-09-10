"""Minimal Delta-table write helpers shared by the batch jobs."""

from __future__ import annotations

import re
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def merge_delta(
    spark: SparkSession,
    updates: DataFrame,
    *,
    table: str,
    keys: tuple[str, ...],
    evolve_schema: bool = False,
) -> None:
    """Create a Delta table or idempotently merge rows into it."""
    parts = table.split(".")
    if len(parts) != 3 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid Unity Catalog table name: {table!r}")
    if not keys or not all(_IDENTIFIER.fullmatch(key) for key in keys):
        raise ValueError("Delta merge keys must be simple identifiers")
    if updates.limit(1).count() == 0:
        raise ValueError(f"Cannot merge an empty update into {table}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {parts[0]}.{parts[1]}")
    if not spark.catalog.tableExists(table):
        updates.write.format("delta").saveAsTable(table)
        return

    view = f"_delu_updates_{uuid4().hex}"
    updates.createOrReplaceTempView(view)
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    merge = "MERGE WITH SCHEMA EVOLUTION" if evolve_schema else "MERGE"
    try:
        spark.sql(
            f"""{merge} INTO {table} AS target
            USING {view} AS source
            ON {condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
        )
    finally:
        spark.catalog.dropTempView(view)
