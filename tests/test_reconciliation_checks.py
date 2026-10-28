import pytest

from src.reconciliation_checks import validate_reconciliation


def test_all_pass():
    result = validate_reconciliation(["PASS", "PASS"])
    assert result == "Reconciliation PASSED: 2 rows validated."


def test_reconciliation_failure():
    with pytest.raises(RuntimeError, match="1 of 2 rows failed"):
        validate_reconciliation(["PASS", "FAIL"])


def test_empty_results():
    with pytest.raises(RuntimeError, match="no results"):
        validate_reconciliation([])


def test_null_status():
    with pytest.raises(RuntimeError):
        validate_reconciliation(["PASS", None])