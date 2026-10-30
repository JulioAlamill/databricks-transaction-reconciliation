# Databricks Transaction Reconciliation

A hands-on data engineering project demonstrating transaction reconciliation with **Databricks**, **Spark Declarative Pipelines (SDP)**, **PySpark**, **Unity Catalog**, **Databricks Asset Bundles**, **Jobs**, automated testing, and **GitHub Actions CI/CD**.

The project runs entirely in **Databricks Free Edition**. DEV, TEST, STAGE, and PROD are simulated environments within the same workspace, not isolated cloud accounts.

## Project overview

The pipeline processes transaction, customer, and control-total data. It standardizes inputs, applies quality rules, identifies duplicate or invalid transactions, checks customer references, and reconciles actual transaction counts and amounts against expected control totals.

The central distinction is between **technical success** (the pipeline runs) and **business success** (the resulting financial reconciliation passes). In PROD, a separate Job task checks the reconciliation output and fails the Job if validation fails or the expected business date has no results.

## Architecture

```text
Environment-specific input CSV files (Unity Catalog Volumes)
                         |
                         v
          Spark Declarative Pipeline (SDP)
                         |
             Raw / standardized data
                         |
       Quality checks and customer enrichment
                         |
            Reconciliation calculations
                         |
        transactions_validation (Delta table)
                         |
               Environment-specific Job
               /                       \
        TEST: integration tests    PROD: business validation
```

**Main technologies:**

- **Spark Declarative Pipelines (SDP):** Defines and executes pipeline dataflows, including materialized views and expectations.
- **PySpark:** Implements reusable transformation logic.
- **Unity Catalog:** Organizes schemas, tables, and source volumes.
- **Databricks Jobs:** Orchestrates pipeline and follow-on test/validation tasks.
- **Databricks Asset Bundles:** Deploys version-controlled resource definitions to selected targets.
- **pytest:** Checks transformation and reconciliation logic and integration outcomes.
- **Git and GitHub:** Version control, feature branches, and Pull Requests.
- **GitHub Actions:** Runs CI checks, deploys to TEST, and supports manually triggered PROD releases.

## Environments

The bundle defines four deployment targets using the same source code and target-specific configuration.

| Target | Output schema | Orchestration |
| --- | --- | --- |
| `dev` | `workspace.transaction_reconciliation_dev` | DEV pipeline and scheduled DEV Job |
| `test` | `workspace.transaction_reconciliation_test` | TEST Job: pipeline, then integration tests |
| `stage` | `workspace.transaction_reconciliation_stage` | STAGE pipeline (no separate Job configured) |
| `prod` | `workspace.transaction_reconciliation_prod` | PROD Job: pipeline, then financial validation |

Input files are organized under `/Volumes/workspace/transaction_reconciliation/landing/<environment>/`. Environment selection is passed through `recon.environment` in the pipeline configuration.

These targets provide **logical separation** for training. They do not represent independently secured production environments.

## Repository structure

```text
.github/workflows/
  ci.yml                       # Unit tests, configuration checks, TEST deployment and integration run
  deploy-prod.yml              # Manually triggered PROD deployment and Job run
resources/
  dev.pipeline.yml             # Shared SDP resource definition (used by all bundle targets)
src/
  transaction_pipeline.py      # SDP pipeline entry point
  transformations.py          # PySpark transformations
  reconciliation_checks.py    # Reusable business validation function
  check_reconciliation.py     # PROD Job entry point for reconciliation validation
tests/
  test_transformations.py     # PySpark transformation unit tests
  test_reconciliation_checks.py # Pure-Python reconciliation unit tests
  test_pipeline_integration.py # Databricks integration test

databricks.yml                # Bundle targets, Jobs, schedules, and dependencies
run_tests.ipynb               # Databricks unit-test runner
run_integration_tests.ipynb   # Databricks integration-test runner
README.md
```

The SDP resource explicitly loads `src/transaction_pipeline.py`, rather than every file in `src/`. This prevents standalone Job scripts from being executed during pipeline initialization.

## Data processing and reconciliation

The pipeline produces a series of materialized views, including:

1. `transactions_raw`, `customers_raw`, and `control_totals_raw` — source datasets.
2. `transactions_clean` — standardized transactions.
3. `transactions_flagged` — transaction validation and duplicate indicators.
4. `transactions_customer_checked` — customer reference validation.
5. `transactions_validation` — count and amount reconciliation against control totals.

`transactions_validation` includes business date, currency, expected and actual counts and amounts, matching indicators, and `validation_status`.

Example training results (datasets vary by environment):

| Environment | Business date | Currency | Transactions | Total | Status |
| --- | --- | --- | ---: | ---: | --- |
| TEST | 2026-10-10 | EUR | 3 | €186.25 | PASS |
| STAGE | 2026-10-11 | EUR | 3 | €129.25 | PASS |
| PROD | 2026-10-12 | EUR | 4 | €310.00 | PASS |

These are **sample data outcomes**, not guaranteed results for every future input dataset.

## Testing strategy

### Pure-Python unit tests

`tests/test_reconciliation_checks.py` tests `validate_reconciliation()` using controlled inputs. Scenarios include all-PASS results, a FAIL status, an empty set, and a null status.

Run locally from the repository root:

```bash
python -m pip install pytest
python -m pytest tests/test_reconciliation_checks.py -v
```

These tests exercise the **real validation function with simulated inputs**; they do not connect to Databricks or test the complete pipeline.

### PySpark transformation unit tests

`tests/test_transformations.py` checks individual transformation functions. These were executed within Databricks using the `run_tests.ipynb` notebook and its Spark-capable environment.

### Integration tests

The Databricks TEST Job `transaction-reconciliation-test` contains two dependent tasks:

1. `run_integration_pipeline` — executes the TEST SDP.
2. `run_integration_tests` — runs `run_integration_tests.ipynb`, which invokes `tests/test_pipeline_integration.py` against TEST outputs.

The integration test checks expected reconciliation results in the TEST environment. An assertion failure causes the test task and overall Job to fail.

### PROD business validation

The PROD Job `transaction-reconciliation-prod-job` runs:

1. `run_reconciliation_pipeline` — executes the PROD SDP.
2. `check_reconciliation` — runs `src/check_reconciliation.py` only after Task 1 succeeds.

The second task checks the records for the specified `--business-date` and calls `validate_reconciliation()`. A non-PASS status or missing results raises an error and fails the Job.

Failure email notifications are configured for the PROD Job and were observed to be delivered during testing.

**Current limitation:** The PROD business date is fixed to `2026-10-12` for the training exercise. A matching `business_date` does not prove the rows were refreshed in the latest pipeline run. A real daily process would require a dynamic expected date and run-level freshness controls.

## Databricks Asset Bundles

Bundle configuration is stored in `databricks.yml` and `resources/dev.pipeline.yml`.

The pipeline resource key is `reconciliation_dev` for all four targets; despite its name, it resolves to the correct environment-specific pipeline. Existing Databricks resources were **bound** to their bundle counterparts, so deployments update those resources instead of creating duplicates.

Examples from VS Code PowerShell (using the existing `training` CLI profile):

```powershell
databricks bundle validate -t test --profile training
databricks bundle plan -t test --profile training
databricks bundle deploy -t test --profile training
databricks bundle run reconciliation_test_job -t test --profile training
```

To inspect a deployed target:

```powershell
databricks bundle summary -t prod --profile training
```

`bundle deploy` creates or updates Databricks resources and uploads code; **it does not itself run the pipeline**. `bundle run` executes a deployed pipeline or Job.

## CI/CD workflow

### Automatic CI and TEST deployment

The workflow `.github/workflows/ci.yml` runs on pushes to `main` and Pull Requests targeting `main`:

- **Python tests:** Execute the reconciliation unit tests.
- **Configuration checks:** Parse YAML and run authenticated Databricks bundle validation for TEST.
- **TEST deployment and integration:** On a push to `main` (not a Pull Request), deploy the checked-out commit to TEST and execute `reconciliation_test_job` after preceding checks succeed.

The CI workflow reports success or failure in GitHub Actions. **A CI pass does not automatically merge a Pull Request**; peer review and merging are separate actions. Direct pushes to `main` are already committed before CI runs.

### Manually triggered PROD deployment

The workflow `.github/workflows/deploy-prod.yml` uses `workflow_dispatch`. A user selects the release reference, enters the required confirmation, and starts the workflow. It validates and plans the PROD bundle, deploys to PROD, then runs the PROD reconciliation Job.

**Release limitation:** The confirmation is not a separate approval gate, and `bundle plan` is informational: the current workflow does not stop for a person to inspect and approve the plan. Users must ensure the chosen commit already passed TEST. This is appropriate for a learning proof of concept, not a production financial release control.

### GitHub authentication

GitHub Actions uses a short-lived Databricks personal access token stored as the GitHub Actions secret `DATABRICKS_TOKEN` and a repository variable `DATABRICKS_HOST`. No token value is committed to Git.

This proof-of-concept token uses the user's permissions; it is **not restricted to TEST by a separate Databricks identity**. In a real organization, prefer dedicated identities, least-privilege permissions, and stronger production approvals.

## Running the project

**From Databricks UI:**

1. Go to **Jobs & Pipelines**.
2. Select `transaction-reconciliation-test` or `transaction-reconciliation-prod-job`.
3. Select **Run now**.
4. Inspect the pipeline task and the subsequent integration-test or business-validation task.

**From GitHub:**

1. Push to `main` to trigger the automatic CI and TEST workflow.
2. Inspect the **Actions** tab and confirm the three CI jobs passed.
3. When deliberately releasing a tested commit, open **Deploy Databricks PROD** and manually trigger the workflow.

The PROD Job's recurring schedule is currently **paused**; the documented PROD run is a deliberate manual execution. The DEV Job has its own configured daily schedule.

## Lessons learned

- A pipeline can complete technically while reconciliation results fail; a follow-on Job task converts business failures into Job failures.
- Unit tests with simulated inputs check edge cases without altering actual datasets; integration tests exercise the Databricks pipeline with TEST data.
- Register only pipeline source files as SDP libraries. Loading every Python file in `src/` caused a standalone `argparse` script to fail during pipeline initialization.
- A bundle's resource keys are logical identifiers. Environment-specific deployment state maps them to real Databricks pipeline and Job IDs.
- Git branches, Pull Requests, CI checks, deployment, and Job execution are distinct stages of a release lifecycle.

## Known limitations and future improvements

- All four targets share one Databricks Free Edition workspace; isolation is logical rather than account-level.
- PROD uses a fixed training business date; dynamic processing-date selection and run-level freshness checks are not implemented.
- No complete historical monitoring dashboard or robust rollback automation is implemented.
- CI does not yet enforce mandatory peer approval or block direct pushes to `main`.
- The manual PROD workflow does not automatically verify the chosen commit passed TEST or pause after showing the deployment plan.
- Existing GitHub Actions versions may emit runtime deprecation warnings and should be updated in a later maintenance pass.

This project is a **training and portfolio proof of concept**, not a production financial processing system.
