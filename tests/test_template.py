from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_spec_exists_before_code():
    assert (ROOT / "docs" / "spec.md").exists()
