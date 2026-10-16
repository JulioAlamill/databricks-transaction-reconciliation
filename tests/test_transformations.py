import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from transformations import add_quality_flags, standardize_transactions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_standardize_and_flag_transactions(spark):
    columns = [
        "transaction_id",
        "customer_id",
        "transaction_timestamp",
        "amount",
        "currency",
        "channel",
        "transaction_type",
    ]

    rows = [
        (" T1001 ", " C001 ", "2026-10-10T09:15:00",
         "125.50", "eur", " ONLINE ", "PAYMENT"),
        ("", "C002", "not-a-date", "-5.00",
         "EUR", "mobile", "payment"),
        ("T1003", "C001", "2026-10-10T11:30:00",
         "18.75", "EUR", "mobile", "payment"),
        ("T1003", "C001", "2026-10-10T11:30:00",
         "18.75", "EUR", "mobile", "payment"),
    ]

    raw = spark.createDataFrame(rows, columns)
    result = add_quality_flags(standardize_transactions(raw)).collect()

    valid = next(row for row in result if row["transaction_id"] == "T1001")
    invalid = next(row for row in result if row["transaction_id"] is None)
    duplicates = [
        row for row in result if row["transaction_id"] == "T1003"
    ]

    assert valid["customer_id"] == "C001"
    assert str(valid["amount"]) == "125.50"
    assert valid["currency"] == "EUR"
    assert valid["channel"] == "online"
    assert valid["is_valid_transaction"] is True
    assert valid["is_duplicate_transaction_id"] is False

    assert invalid["valid_transaction_id"] is False
    assert invalid["valid_timestamp"] is False
    assert invalid["valid_amount"] is False
    assert invalid["is_valid_transaction"] is False

    assert len(duplicates) == 2
    assert all(row["is_duplicate_transaction_id"] is True for row in duplicates)