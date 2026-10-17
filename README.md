# Databricks Transaction Reconciliation

A hands-on data engineering project demonstrating transaction reconciliation using Databricks, Spark Declarative Pipelines (SDP), PySpark, Unity Catalog and automated testing.

## Project Overview

The pipeline processes transaction and customer data, applies data quality rules, identifies invalid or duplicate transactions, and reconciles the processed results against expected control totals.

## Architecture

The project uses:

- **Spark Declarative Pipelines (SDP)** to define and execute transformations.
- **Unity Catalog** to manage tables and environment-specific datasets.
- **PySpark** for reusable transformation logic.
- **SDP Expectations** to validate data quality.
- **pytest** for unit and integration testing.
- **Databricks Jobs** to orchestrate pipeline execution and automated tests.
- **Git** for version control.

## Testing

### Unit Tests

File: `tests/test_transformations.py`

Tests individual transformation functions independently of the full pipeline.

Run using the Databricks notebook `run_tests`.

### Integration Tests

File: `tests/test_pipeline_integration.py`

Validates the output of the complete test pipeline, including:

- Reconciliation status and control totals.
- Transaction counts and amounts.
- Duplicate transaction identification.
- Customer reference validation.
- Invalid transaction identification.

Run using the notebook `run_integration_tests`.

## Test Environment

Pipeline: `transaction-reconciliation-integration-test`

Configuration:

- Environment: `test`
- Source: `/Volumes/workspace/transaction_reconciliation/landing/test/`
- Target schema: `workspace.transaction_reconciliation_test`

The test dataset contains a controlled set of transactions with known expected outcomes.

The reconciliation test verifies that 3 valid transactions reconcile to a total of EUR 186.25.

## Automated Job

Job: `transaction-reconciliation-test`

The Job contains two dependent tasks:

1. `run_integration_pipeline` — executes the SDP test pipeline.
2. `run_integration_tests` — runs pytest against the resulting tables, only if the pipeline succeeds.

If any integration assertion fails, the second task fails and the Job reports failure.

## How to Run

1. Open Databricks **Jobs & Pipelines**.
2. Select `transaction-reconciliation-test`.
3. Click **Run now**.
4. Verify that both tasks complete successfully.
5. Open the `run_integration_tests` task output to inspect pytest results.

Unit tests can be executed independently using the `run_tests` notebook.

## Future Improvements

- Extend environment-specific deployment to development, staging and production.
- Introduce Declarative Automation Bundles for deployment automation.
- Add CI/CD checks and automated validation as part of the development workflow.