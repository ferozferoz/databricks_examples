"""Example Python UDFs.

The plain-Python logic lives in ``_``-prefixed functions so it can be unit-tested
without Spark; the public ``*_udf`` objects wrap them for use in DataFrames.

Note: prefer built-in Spark functions where they exist (e.g. ``parse_url``);
UDFs are here to show the pattern for logic Spark can't express natively.
"""

from __future__ import annotations

from urllib.parse import urlparse

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def _extract_domain(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url if "//" in url else f"//{url}").hostname
    return host.removeprefix("www.") if host else None


def _url_length_bucket(length: int | None) -> str | None:
    if length is None:
        return None
    if length < 50:
        return "short"
    if length < 150:
        return "medium"
    return "long"


extract_domain_udf = udf(_extract_domain, StringType())
url_length_bucket_udf = udf(_url_length_bucket, StringType())
