"""Configuration helpers.

Why this module exists:
    Thresholds, table names and bucket prefixes change between environments and over time.
    Keeping them out of the business logic (no "magic numbers") means a change of rule is a
    change of configuration, not a code change, and every value has a descriptive name.

How it is used:
    Lambdas and jobs call `get_str`, `get_int` or `get_float` with an environment variable
    name and a default. Terraform sets the real values as Lambda/Glue environment variables.
"""

from __future__ import annotations

import os


def get_str(name: str, default: str | None = None) -> str:
    """Return a required text setting from the environment.

    Args:
        name: Environment variable name, e.g. "ALERT_TOPIC_ARN".
        default: Value to use when the variable is not set. None means "required".

    Returns:
        The configured value.

    Raises:
        KeyError: If the variable is missing and no default was given. Failing here, at
            start-up, is better than failing halfway through processing a batch.
    """
    # Read the variable; fall back to the default only if one was provided
    value = os.environ.get(name, default)

    # A missing required setting is a deployment error: stop with a clear message
    if value is None:
        raise KeyError(f"Missing required environment variable: {name}")
    return value


def get_int(name: str, default: int) -> int:
    """Return an integer setting (e.g. a threshold in units) from the environment.

    Args:
        name: Environment variable name.
        default: Value used when the variable is not set.

    Returns:
        The value converted to int.
    """
    # Environment variables are always text, so convert explicitly
    return int(os.environ.get(name, default))


def get_float(name: str, default: float) -> float:
    """Return a decimal setting (e.g. a percentage threshold) from the environment.

    Args:
        name: Environment variable name.
        default: Value used when the variable is not set.

    Returns:
        The value converted to float.
    """
    # Same as get_int, for thresholds like "3.5 percent"
    return float(os.environ.get(name, default))
