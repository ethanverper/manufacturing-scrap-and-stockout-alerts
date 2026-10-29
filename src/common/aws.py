"""Single place where AWS clients are created.

Why:
    Business logic should never create clients or resources itself. Creating them here, lazily and once,
    means:
      - Lambdas reuse the same client across invocations (faster, fewer connections);
      - unit tests replace `get_client` with a fake and never touch AWS (no cost, no credentials).
"""

from __future__ import annotations

from functools import cache
from typing import Any


@cache
def get_client(service: str) -> Any:
    """Return a cached boto3 client for an AWS service.

    Args:
        service: AWS service name, e.g. "sns", "s3", "kinesis".

    Returns:
        A boto3 client. The same object is returned on every later call (thanks to @cache).
    """
    # Imported here, not at the top, so modules that only use pure logic (and their tests)
    # don't need boto3 installed
    import boto3

    return boto3.client(service)


@cache
def get_resource(service: str) -> Any:
    """Return a cached boto3 *resource* (higher-level API) for an AWS service.

    Why both clients and resources: DynamoDB's resource layer converts plain Python values
    (str, int, Decimal) to DynamoDB's typed format automatically, so code reads like normal
    Python instead of {"S": "..."} / {"N": "..."} dictionaries.

    Args:
        service: AWS service name, e.g. "dynamodb".

    Returns:
        A boto3 service resource, cached like get_client.
    """
    # Same lazy import as get_client: pure-logic modules never need boto3
    import boto3

    return boto3.resource(service)
