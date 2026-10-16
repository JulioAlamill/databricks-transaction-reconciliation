from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations import (
    add_quality_flags,
    add_customer_details,
    calculate_actual_totals,
    reconcile_control_totals,
    standardize_transactions,
)


def input_file(filename):
    """Build the landing path for the selected environment."""
    environment = spark.conf.get("recon.environment", "dev").lower()

    if environment not in {"dev", "stage", "prod", "test"}:
        raise ValueError(
            "recon.environment must be dev, stage, or prod"
        )

    root = (
        "/Volumes/workspace/transaction_reconciliation/"
        f"landing/{environment}"
    )
    return f"{root}/{filename}"


@dp.materialized_view(name="transactions_raw")
def transactions_raw():
    """Read the transaction CSV files for the selected environment."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_file("transactions/"))
    )


@dp.materialized_view(name="transactions_clean")
def transactions_clean():
    """Standardize transaction fields and data types."""
    return standardize_transactions(
        spark.read.table("transactions_raw")
    )


@dp.materialized_view(name="transactions_flagged")
@dp.expect(
    "all_transaction_fields_valid",
    "is_valid_transaction = true",
)
@dp.expect(
    "no_duplicate_transaction_ids",
    "is_duplicate_transaction_id = false",
)
def transactions_flagged():
    """Add transaction quality flags and report expectation failures."""
    return add_quality_flags(
        spark.read.table("transactions_clean")
    )


@dp.materialized_view(name="control_totals_raw")
def control_totals_raw():
    """Read the expected control totals for the selected environment."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_file("control_totals/"))
    )


@dp.materialized_view(name="customers_raw")
def customers_raw():
    """Read customer reference data and trim its string fields."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_file("customers/"))
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("segment")).alias("segment"),
            F.trim(F.col("status")).alias("status"),
        )
    )


@dp.materialized_view(name="transactions_customer_checked")
@dp.expect(
    "customer_id_exists",
    "customer_exists = true",
)
def transactions_customer_checked():
    """Add customer match details and flag unmatched customer IDs."""
    transactions = spark.read.table("transactions_flagged")
    customers = spark.read.table("customers_raw")

    return add_customer_details(transactions, customers)


@dp.materialized_view(name="transactions_validation")
def transactions_validation():
    """Compare actual transaction totals with the supplied control totals."""

    # Calculate totals from valid transactions with a matching customer.
    # The helper also removes duplicate transaction IDs before aggregation.
    actual = calculate_actual_totals(
        spark.read.table("transactions_customer_checked")
    )

    # Convert the raw control fields to the types needed for comparison.
    controls = (
        spark.read.table("control_totals_raw")
        .select(
            F.to_date("business_date").alias("business_date"),
            F.col("expected_transaction_count").cast("long"),
            F.col("expected_total_amount").cast("decimal(18,2)"),
            "currency",
        )
    )

    # Compare expected and actual counts and amounts.
    return reconcile_control_totals(controls, actual)