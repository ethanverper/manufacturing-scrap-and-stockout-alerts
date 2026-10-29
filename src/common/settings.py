"""Load the project's central configuration files (config/settings.yaml, config/master_data.yaml).

Why this module exists:
    Every tunable value (thresholds, simulation parameters, AWS sizing) and all master data live
    in the config/ folder, so nobody has to hunt through modules to change a rule. This module is
    the only place that reads those files; every other module asks it.

Where it is used:
    Code that runs locally (generators, scripts). Code that runs in AWS (Lambda, Glue) does not
    read YAML: Terraform reads the same files and passes the values as environment variables or
    job arguments, which those components read through src/common/config.py.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Any

import yaml

# Repository root: src/common/settings.py -> three levels up
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"


@cache
def load_yaml(file_name: str) -> dict[str, Any]:
    """Read and parse one YAML file from config/, once per process.

    Args:
        file_name: File name inside config/, e.g. "settings.yaml".

    Returns:
        The parsed content as nested dicts and lists.

    Raises:
        FileNotFoundError: If the file doesn't exist (a broken checkout should fail loudly).
    """
    # safe_load builds only plain data types (never executes code embedded in the file)
    with (CONFIG_DIR / file_name).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def settings() -> dict[str, Any]:
    """Return config/settings.yaml: thresholds, plant layout, simulation and AWS sizing."""
    return load_yaml("settings.yaml")


def master_data() -> dict[str, Any]:
    """Return config/master_data.yaml: suppliers, parts and bill of materials."""
    return load_yaml("master_data.yaml")
