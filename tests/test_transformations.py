import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datetime import date, datetime
from decimal import Decimal

from transformations import (
    add_customer_details,
    add_quality_flags,
    calculate_actual_totals,
    reconcile_control_totals,
    standardize_transactions,
)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.getOrCreate()
    # Disable ANSI mode so to_timestamp returns NULL for malformed input
    # instead of throwing DateTimeException
    
    return spark


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


def test_add_customer_details(spark):
    transactions = spark.createDataFrame(
        [("T1001", "C001"), ("T1006", "C999")],
        ["transaction_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [("C001", "retail", "active")],
        ["customer_id", "segment", "status"],
    )

    result = add_customer_details(transactions, customers).collect()
    by_id = {row["transaction_id"]: row for row in result}

    assert by_id["T1001"]["customer_exists"] is True
    assert by_id["T1001"]["customer_segment"] == "retail"
    assert by_id["T1001"]["customer_status"] == "active"

    assert by_id["T1006"]["customer_exists"] is False
    assert by_id["T1006"]["customer_segment"] is None


def test_calculate_actual_totals(spark):
    # T1003 appears twice; it should only be counted once.
    transactions = spark.createDataFrame(
        [
            ("T1001", datetime(2026, 10, 10, 9, 15), Decimal("125.50"),
             "EUR", True, True),
            ("T1002", datetime(2026, 10, 10, 10, 2), Decimal("42.00"),
             "EUR", True, True),
            ("T1003", datetime(2026, 10, 10, 11, 30), Decimal("18.75"),
             "EUR", True, True),
            ("T1003", datetime(2026, 10, 10, 11, 30), Decimal("18.75"),
             "EUR", True, True),
            # Excluded because the customer does not match.
            ("T1006", datetime(2026, 10, 10, 14, 20), Decimal("70.00"),
             "EUR", True, False),
            # Excluded because the transaction is invalid.
            ("T1007", datetime(2026, 10, 10, 14, 30), Decimal("12.00"),
             "EUR", False, True),
        ],
        [
            "transaction_id",
            "transaction_timestamp",
            "amount",
            "currency",
            "is_valid_transaction",
            "customer_exists",
        ],
    )

    result = calculate_actual_totals(transactions).collect()

    assert len(result) == 1
    assert result[0]["actual_transaction_count"] == 3
    assert result[0]["actual_total_amount"] == Decimal("186.25")


def test_reconcile_control_totals(spark):
    controls = spark.createDataFrame(
        [
            (date(2026, 10, 10), "EUR", 3, Decimal("186.25")),
            # This control row deliberately has the wrong count.
            (date(2026, 10, 10), "EUR", 4, Decimal("186.25")),
        ],
        [
            "business_date",
            "currency",
            "expected_transaction_count",
            "expected_total_amount",
        ],
    )

    actual = spark.createDataFrame(
        [
            (date(2026, 10, 10), "EUR", 3, Decimal("186.25")),
        ],
        [
            "business_date",
            "currency",
            "actual_transaction_count",
            "actual_total_amount",
        ],
    )

    result = reconcile_control_totals(controls, actual).collect()
    by_expected_count = {
        row["expected_transaction_count"]: row for row in result
    }

    assert by_expected_count[3]["validation_status"] == "PASS"
    assert by_expected_count[4]["validation_status"] == "FAIL"