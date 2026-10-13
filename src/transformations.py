from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_transactions(df: DataFrame) -> DataFrame:
    """Clean column formats and convert raw CSV values to usable types."""
    transaction_id = F.trim(F.col("transaction_id"))
    customer_id = F.trim(F.col("customer_id"))

    return df.select(
        F.when(F.length(transaction_id) > 0, transaction_id)
        .otherwise(F.lit(None).cast("string"))
        .alias("transaction_id"),

        F.when(F.length(customer_id) > 0, customer_id)
        .otherwise(F.lit(None).cast("string"))
        .alias("customer_id"),

        F.try_to_timestamp(
            F.col("transaction_timestamp"),
            F.lit("yyyy-MM-dd'T'HH:mm:ss"),
        ).alias("transaction_timestamp"),

        F.expr("try_cast(amount AS DECIMAL(12,2))").alias("amount"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),
        F.lower(F.trim(F.col("channel"))).alias("channel"),
        F.lower(F.trim(F.col("transaction_type"))).alias("transaction_type"),
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Add row-level flags; customer matching is checked later in the pipeline."""
    flagged = (
        df
        .withColumn("valid_transaction_id", F.col("transaction_id").isNotNull())
        .withColumn("valid_customer_id", F.col("customer_id").isNotNull())
        .withColumn("valid_timestamp", F.col("transaction_timestamp").isNotNull())
        .withColumn("valid_amount", F.col("amount") > 0)
        .withColumn("valid_currency", F.col("currency").rlike("^[A-Z]{3}$"))
        .withColumn(
            "valid_channel",
            F.col("channel").isin("online", "branch", "mobile"),
        )
        .withColumn(
            "valid_transaction_type",
            F.col("transaction_type").isin("payment", "withdrawal", "transfer"),
        )
    )

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