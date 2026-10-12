# Financial Transactions & Reconciliation Pipeline

A learning project that processes **synthetic** daily banking transactions with Databricks Lakeflow Spark Declarative Pipelines.

## Business question

Did the transaction file received for each business date agree with its control totals, and which rows need investigation?

## Planned data flow

```text
Synthetic CSV files
  → Bronze: landed transaction rows with source metadata
  → Silver: typed, standardised, deduplicated transactions and quality rules
  → Gold: daily reconciliation totals and exception views
```

The same pipeline code will be configured for development, staging, and production. Each environment will use its own input data and output schema/catalog settings. This first version uses small batch files; incremental file ingestion can follow once the core pipeline and tests work.

## Repository layout

- `data/`: small, synthetic source files for dev, stage, and prod
- `src/`: pipeline code and reusable transformations (to be added next)
- `tests/`: PySpark unit tests (to be added with the transformations)
- `resources/`: Lakeflow pipeline and job resources for DAB deployment (added after the pipeline works)
- `databricks.yml`: DAB targets for dev, stage, and prod (added during deployment work)

## Data quality examples

The dev sample intentionally includes a duplicate transaction, a blank transaction ID, a non-positive amount, an invalid date, and an unknown customer. We will use these to prove that expectations and tests behave as intended. Source data is synthetic and contains no real customer or account information.

## Build sequence

1. Inspect the sample data and define the cleaning rules.
2. Implement reusable PySpark transformations and unit tests.
3. Build the declarative Bronze → Silver → Gold pipeline and add expectations.
4. Add integration-test validation views using controlled valid and invalid rows.
5. Connect GitHub and deploy the same code to dev, stage, and prod with DABs.
