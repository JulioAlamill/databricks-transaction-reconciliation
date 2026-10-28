from pyspark.sql import SparkSession
from reconciliation_checks import validate_reconciliation

spark = SparkSession.builder.getOrCreate()

table = "workspace.transaction_reconciliation_prod.transactions_validation"

df = spark.table(table)

statuses = [row.validation_status for row in df.select("validation_status").collect()]

result = validate_reconciliation(statuses)

print(result)



import argparse
from pyspark.sql import SparkSession
from reconciliation_checks import validate_reconciliation

parser = argparse.ArgumentParser()
parser.add_argument("--business-date", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

table = "workspace.transaction_reconciliation_prod.transactions_validation"

df = spark.table(table)

statuses = [
    row.validation_status
    for row in df.filter(
        df.business_date == args.business_date
    ).select("validation_status").collect()
]

result = validate_reconciliation(statuses)

print(f"Business date: {args.business_date}")
print(result)