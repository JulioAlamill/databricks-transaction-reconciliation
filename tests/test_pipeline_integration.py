import pytest
from pyspark.sql import SparkSession, functions as F


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_test_pipeline_outputs(spark):
    schema = "workspace.transaction_reconciliation_test"

    validation = spark.table(f"{schema}.transactions_validation").collect()
    assert len(validation) == 1

    result = validation[0]
    assert result["actual_transaction_count"] == 3
    assert str(result["actual_total_amount"]) == "186.25"
    assert result["count_matches"] is True
    assert result["amount_matches"] is True
    assert result["validation_status"] == "PASS"

    transactions = spark.table(
        f"{schema}.transactions_customer_checked"
    )

    assert transactions.count() == 8
    
    assert transactions.filter(
    F.col("is_duplicate_transaction_id")
    ).count() == 2

    assert transactions.filter(
        ~F.col("customer_exists")
    ).count() == 1

    assert transactions.filter(
        ~F.col("is_valid_transaction")
    ).count() == 3

    assert transactions.filter(
        F.col("is_duplicate_transaction_id")
    ).count() == 2
    assert transactions.filter(
        ~F.col("customer_exists")
    ).count() == 1
    assert transactions.filter(
        ~F.col("is_valid_transaction")
    ).count() == 3