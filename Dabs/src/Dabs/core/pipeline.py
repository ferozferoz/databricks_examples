"""Abstract base class that every pipeline implements.

business logic will be implemented in child classes that implements ABC
this class covers :
  * Spark session creation (Databricks Connect locally, the active session on Databricks)
  * Command-line parameter parsing (common args + a per-pipeline hook)
  * Logging, timing and error handling around ``run()``
"""

from __future__ import annotations

import argparse
import logging
import time
from abc import ABC, abstractmethod
from typing import Sequence

from pyspark.sql import SparkSession


class Pipeline(ABC):
    """Template-method base class for all data pipelines.

    Subclasses MUST implement :meth:`run` and MAY override :meth:`add_arguments`.
    """

    #: Used as logger name, argparse prog and audit column value.
    name: str = "pipeline"

    def __init__(self, args: argparse.Namespace, spark: SparkSession | None = None) -> None:
        self.args = args
        self.log = logging.getLogger(self.name)
        self.spark = spark or self.create_spark_session()

    # ------------------------------------------------------------------ spark
    @staticmethod
    def create_spark_session() -> SparkSession:
        """Return a Spark session that works locally and on Databricks.

        - Serverless / local dev with databricks-connect: DatabricksSession.
        - Classic cluster without databricks-connect: the already-active SparkSession.
        """
        try:
            from databricks.connect import DatabricksSession

            return DatabricksSession.builder.getOrCreate()
        except ImportError:
            return SparkSession.builder.getOrCreate()

    # ---------------------------------------------------------------- parsing
    @classmethod
    def build_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=cls.name, description=cls.__doc__)
        parser.add_argument("--catalog", required=True, help="Target Unity Catalog catalog")
        parser.add_argument("--schema", required=True, help="Target schema inside the catalog")
        parser.add_argument(
            "--log_level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            help="Python logging level",
        )
        cls.add_arguments(parser)
        return parser

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Hook: register pipeline-specific arguments. Default is no extra args."""

    # -------------------------------------------------------------- lifecycle
    @abstractmethod
    def run(self) -> None:
        """Pipeline business logic: read -> transform -> write."""

    def execute(self) -> None:
        """Run the pipeline with uniform logging, timing and failure reporting."""
        self.log.info("Starting pipeline '%s' with args: %s", self.name, vars(self.args))
        started = time.perf_counter()
        try:
            self.run()
        except Exception:
            self.log.exception("Pipeline '%s' failed", self.name)
            raise
        self.log.info("Pipeline '%s' finished in %.1fs", self.name, time.perf_counter() - started)

    @classmethod
    def main(cls, argv: Sequence[str] | None = None) -> None:
        """Entry point used by the ``[project.scripts]`` console scripts."""
        args = cls.build_parser().parse_args(argv)
        logging.basicConfig(
            level=args.log_level,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )
        cls(args).execute()
