from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def standardize_transactions(df: DataFrame) -> DataFrame:
    """Trim transaction fields and convert them to consistent formats."""

    # Remove spaces from the start and end of these string fields.
    string_columns = [
        "transaction_id",
        "customer_id",
        "transaction_timestamp",
        "currency",
        "channel",
        "transaction_type",
    ]

    for column in string_columns:
        df = df.withColumn(column, F.trim(F.col(column)))

    # Treat blank transaction and customer IDs as missing values (NULL).
    for column in ["transaction_id", "customer_id"]:
        df = df.withColumn(
            column,
            F.when(
                F.col(column).isNull() | (F.col(column) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col(column)),
        )

    # Convert values into standard formats:
    # - Parse the timestamp; blank strings and "-" become NULL.
    # - Convert amount to a decimal with two places.
    # - Use uppercase for currency and lowercase for channel and transaction type.
    return (
        df
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.when(
                    ~F.col("transaction_timestamp").isin("", "-"),
                    F.col("transaction_timestamp"),
                )
            ),
        )
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("currency", F.upper(F.col("currency")))
        .withColumn("channel", F.lower(F.col("channel")))
        .withColumn("transaction_type", F.lower(F.col("transaction_type")))
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Add flags showing whether each transaction field passes its checks."""

    # Create one Boolean flag for each field-level validation rule.
    # coalesce(..., False) makes a NULL check result count as False.
    flagged = (
        df
        .withColumn("valid_transaction_id", F.col("transaction_id").isNotNull())
        .withColumn("valid_customer_id", F.col("customer_id").isNotNull())
        .withColumn("valid_timestamp", F.col("transaction_timestamp").isNotNull())
        .withColumn(
            "valid_amount",
            F.coalesce(F.col("amount") > 0, F.lit(False)),
        )
        .withColumn(
            "valid_currency",
            F.coalesce(F.col("currency").rlike("^[A-Z]{3}$"), F.lit(False)),
        )
        .withColumn(
            "valid_channel",
            F.coalesce(
                F.col("channel").isin("online", "branch", "mobile"),
                F.lit(False),
            ),
        )
        .withColumn(
            "valid_transaction_type",
            F.coalesce(
                F.col("transaction_type").isin(
                    "payment", "withdrawal", "transfer"
                ),
                F.lit(False),
            ),
        )
    )

    # Define a window containing rows with the same transaction_id.
    # Counting within this window lets us identify IDs appearing more than once.
    transaction_id_count = Window.partitionBy("transaction_id")

    flagged = (
        flagged
        .withColumn(
            "_transaction_id_count",
            F.count("transaction_id").over(transaction_id_count),
        )
        # Mark every row with a repeated, non-null ID as a duplicate.
        .withColumn(
            "is_duplicate_transaction_id",
            F.col("transaction_id").isNotNull()
            & (F.col("_transaction_id_count") > 1),
        )
        # This count is only an intermediate calculation, so remove it.
        .drop("_transaction_id_count")
    )

    # A transaction is valid when all the listed field checks pass.
    # Duplicate status and customer matching are separate checks.
    return flagged.withColumn(
        "is_valid_transaction",
        F.col("valid_transaction_id")
        & F.col("valid_customer_id")
        & F.col("valid_timestamp")
        & F.col("valid_amount")
        & F.col("valid_currency")
        & F.col("valid_channel")
        & F.col("valid_transaction_type"),
    )


def calculate_actual_totals(transactions: DataFrame) -> DataFrame:
    """Aggregate valid, customer-matched, unique transactions by date/currency."""

    # Keep transactions that passed the field checks and matched a customer.
    # Keep one row per transaction ID so duplicates are not counted twice.
    # Derive business_date from the transaction timestamp for grouping.
    valid_unique = (
        transactions
        .filter(
            F.col("is_valid_transaction")
            & F.col("customer_exists")
        )
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "business_date",
            F.to_date("transaction_timestamp"),
        )
    )

    # Calculate the transaction count and amount for each date and currency.
    return (
        valid_unique
        .groupBy("business_date", "currency")
        .agg(
            F.count("*").alias("actual_transaction_count"),
            F.sum("amount")
            .cast("decimal(18,2)")
            .alias("actual_total_amount"),
        )
    )


def reconcile_control_totals(
    controls: DataFrame,
    actual: DataFrame,
) -> DataFrame:
    """Compare actual transaction totals with the supplied control totals."""

    # Match actual totals to controls using the date and currency.
    # A left join keeps every control row, even if no actual total matches it.
    result = controls.join(
        actual,
        ["business_date", "currency"],
        "left",
    )

    return (
        result
        # If no actual row matched, treat its transaction count as zero.
        # Compare that count with the expected count from the controls.
        .withColumn(
            "count_matches",
            F.coalesce(F.col("actual_transaction_count"), F.lit(0))
            == F.col("expected_transaction_count"),
        )
        # Treat a missing actual amount as zero, then compare amounts
        # rounded to two decimal places.
        .withColumn(
            "amount_matches",
            F.round(
                F.coalesce(F.col("actual_total_amount"), F.lit(0)), 2
            )
            == F.round(F.col("expected_total_amount"), 2),
        )
        # PASS requires both the count and amount to match.
        .withColumn(
            "validation_status",
            F.when(
                F.col("count_matches") & F.col("amount_matches"),
                "PASS",
            ).otherwise("FAIL"),
        )
    )



def add_customer_details(
    transactions: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Add customer match, segment, and status columns to transactions."""

    customer_lookup = (
        customers
        .select(
            "customer_id",
            F.col("segment").alias("customer_segment"),
            F.col("status").alias("customer_status"),
        )
        # Prevent duplicate customer IDs from multiplying transaction rows.
        .dropDuplicates(["customer_id"])
        .withColumn("_customer_match", F.lit(True))
    )

    return (
        transactions
        .join(customer_lookup, on="customer_id", how="left")
        .withColumn(
            "customer_exists",
            F.coalesce(F.col("_customer_match"), F.lit(False)),
        )
        .drop("_customer_match")
    )


    