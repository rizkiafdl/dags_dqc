from dags.results import ValidationResult

def uniqueness_check(context):
    df = context.df
    dup_count = df.duplicated(subset=["customer_id"]).sum()
    passed = dup_count == 0
    return ValidationResult(
        "uniqueness",
        passed,
        metrics={"duplicate_count": dup_count}
    )
