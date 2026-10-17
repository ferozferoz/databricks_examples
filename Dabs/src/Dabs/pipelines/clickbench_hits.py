"""Top domains by hit count from the ClickBench sample data."""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from Dabs.core import Pipeline
from Dabs.utils import (
    add_audit_columns,
    extract_domain_udf,
    resolve_column,
    top_n_by_count,
    url_length_bucket_udf,
)


class ClickBenchHitsPipeline(Pipeline):
    """Reads clickbench hits, derives domain + URL-length bucket, writes top-N domains."""

    name = "clickbench_hits"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--source_table", default="samples.clickbench.hits")
        parser.add_argument("--target_table", default="clickbench_top_domains")
        parser.add_argument("--top_n", type=int, default=100)
        parser.add_argument("--limit", type=int, default=None, help="Row cap for quick test runs")

    def run(self) -> None:
        hits = self.spark.read.table(self.args.source_table)
        if self.args.limit:
            hits = hits.limit(self.args.limit)

        url_col = resolve_column(hits, "URL")
        enriched = (
            hits.select(F.col(url_col).alias("url"))
            .where(F.col("url") != "")
            .withColumn("domain", extract_domain_udf("url"))
            .withColumn("url_length_bucket", url_length_bucket_udf(F.length("url")))
            .where(F.col("domain").isNotNull())
        )

        summary = add_audit_columns(top_n_by_count(enriched, ["domain"], self.args.top_n), self.name)

        target = f"{self.args.catalog}.{self.args.schema}.{self.args.target_table}"
        summary.write.mode("overwrite").saveAsTable(target)
        self.log.info("Wrote %s", target)


def main() -> None:
    ClickBenchHitsPipeline.main()
