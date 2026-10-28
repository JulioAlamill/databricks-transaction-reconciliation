def validate_reconciliation(statuses):
    if not statuses:
        raise RuntimeError("Reconciliation produced no results.")

    failed = sum(status != "PASS" for status in statuses)

    if failed:
        raise RuntimeError(
            f"Reconciliation FAILED: {failed} of {len(statuses)} rows failed."
        )

    return f"Reconciliation PASSED: {len(statuses)} rows validated."