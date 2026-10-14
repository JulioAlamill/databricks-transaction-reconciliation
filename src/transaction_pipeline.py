from pyspark import pipelines as dp
from transformations import standardize_transactions, add_quality_flags


def input_file(filename):
    environment = spark.conf.get("recon.environment", "dev").lower()

    if environment not in {"dev", "stage", "prod"}:
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
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_file("transactions/"))
    )


@dp.materialized_view(name="transactions_clean")
def transactions_clean():
    return standardize_transactions(
        spark.read.table("transactions_raw")
    )


@dp.materialized_view(name="transactions_flagged")
def transactions_flagged():
    return add_quality_flags(
        spark.read.table("transactions_clean")
    )