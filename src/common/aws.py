"""Single place where AWS clients are created.

Why:
    Business logic should never create clients itself. Creating them here, lazily and once,
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
