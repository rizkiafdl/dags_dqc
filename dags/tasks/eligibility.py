from dateutil.relativedelta import relativedelta
from datetime import datetime
from dags.results import ValidationResult

def compute_age(dob):
    dob = datetime.strptime(dob, "%Y-%m-%d")
    return relativedelta(datetime.now(), dob).years

def eligibility_check(df):
    df["age"] = df["date_of_birth"].apply(compute_age)

    invalid = df[(df["age"] <= 17) | (df["user_status"] != "active")]

    passed = len(invalid) == 0

    return ValidationResult(
        "eligibility",
        passed,
        metrics={"invalid_count": len(invalid)},
        details={"invalid_users": invalid[["customer_id", "age", "user_status"]].to_dict(orient="records")}
    )
