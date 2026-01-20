import re
from dags.results import ValidationResult
from datetime import datetime

email_re = re.compile(r"[^@]+@[^@]+\.[^@]+")

def anomalies_check(context):
    df = context.df
    
    anomalies = {}
    nulls = df.isna().sum().to_dict()
    anomalies["null_counts"] = nulls

    # invalid email
    invalid_emails = df[~df["email"].apply(lambda x: bool(email_re.match(str(x))))]
    anomalies["invalid_email_count"] = len(invalid_emails)

    # paradox: signup before birth
    paradox = []
    for _, r in df.iterrows():
        dob = datetime.strptime(r["date_of_birth"], "%Y-%m-%d")
        signup = datetime.strptime(r["signup_date"], "%Y-%m-%d")
        if signup <= dob:
            paradox.append(r["customer_id"])
    anomalies["signup_before_birth"] = paradox

    return ValidationResult(
        "anomalies",
        passed=len(paradox)==0 and len(invalid_emails)==0,
        metrics={"invalid_email": len(invalid_emails),
                 "signup_birth_paradox": len(paradox)},
        details=anomalies
    )
