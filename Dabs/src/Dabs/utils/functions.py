"""Reusable DataFrame helper functions (pure Spark, no UDFs)."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolve_column(df: DataFrame, name: str) -> str:
    """Return the actual column name matching ``name`` case-insensitively."""
    lookup = {c.lower(): c for c in df.columns}
    try:
        return lookup[name.lower()]
    except KeyError:
        raise ValueError(f"Column '{name}' not found. Available: {df.columns}") from None


def add_audit_columns(df: DataFrame, pipeline_name: str) -> DataFrame:
    """Append lineage columns: producing pipeline and processing timestamp."""
    return df.withColumn("_pipeline", F.lit(pipeline_name)).withColumn(
        "_processed_at", F.current_timestamp()
    )


def top_n_by_count(df: DataFrame, group_cols: Sequence[str], n: int = 100) -> DataFrame:
    """Group by ``group_cols`` and return the ``n`` most frequent groups."""
    return df.groupBy(*group_cols).agg(F.count("*").alias("hits")).orderBy(F.desc("hits")).limit(n)
