from __future__ import annotations

import time
from urllib.parse import urlparse

import boto3
import requests

REQUEST_TIMEOUT_SECONDS = 30


def stage_payload(payload_text: str, staging_path: str, season: str, round_: str) -> str:
    """Writes payload_text as a single JSON file under staging_path (an s3:// URI
    prefix), using a key unique to this (season, round, timestamp) so repeated
    runs never collide and a streaming file source always sees each run's file
    as new.

    Pure boto3 -- no Spark API involved, so this can't trip the Spark-Connect
    pipeline-query-function analyze/execute block (confirmed on a real Glue run
    that raw_race's bronze flow must return a genuine streaming relation, not a
    batch DataFrame built in-process: AnalysisException
    [INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE]). Staging to S3
    first and reading it back via spark.readStream is what makes the returned
    DataFrame a real streaming relation.

    Returns the full s3:// URI written.
    """
    parsed = urlparse(staging_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    key = f"{prefix}{season}_{round_}_{int(time.time() * 1000)}.json"
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=payload_text.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def fetch_race_payload(base_url: str, season: str | None, round_: str | None) -> str:
    """Fetch one race's raw JSON payload from the F1 API.

    One (season, round) per call, by design -- "one run = one race" mirrors the
    Databricks original's ingestion granularity. There is no quarantine/retry
    layer in v1: a missing conf, network/HTTP failure, or non-2xx response simply
    raises, which fails the job run (acceptable for a single-race-per-run hobby
    pipeline where you just re-run it). DQ/malformed-record handling is deferred
    to a later pass.
    """
    if not season or not round_:
        raise ValueError(
            "f1.season.year and f1.season.round are both required --conf arguments"
        )

    url = f"{base_url}/{season}/{round_}/race"
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text
