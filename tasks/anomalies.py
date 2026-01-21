import re
from datetime import date
import pandas as pd
from dags.results import ValidationResult

email_re = re.compile(r"[^@]+@[^@]+\.[^@]+")
MIN_YEAR = 2018

def anomalies_check(context):
    df = context.df
    today = date.today()
    cutoff = date(MIN_YEAR, 1, 1)

    anomalies = {}

    # completeness checks
    completeness = {
        "null_first_name": df["first_name"].isna().sum(),
        "null_last_name": df["last_name"].isna().sum(),
        "null_customer_id": df["customer_id"].isna().sum(),
        "null_email": df["email"].isna().sum(),
        "null_dob": df["date_of_birth"].isna().sum(),
        "null_signup_date": df["signup_date"].isna().sum(),
    }
    anomalies["completeness"] = completeness

    # email validity
    invalid_emails = df[~df["email"].astype(str).apply(lambda x: bool(email_re.match(x)))]
    anomalies["invalid_email_count"] = len(invalid_emails)

    # date parsing
    signup_pd = pd.to_datetime(df["signup_date"], errors="coerce")
    dob_pd = pd.to_datetime(df["date_of_birth"], errors="coerce")

    # normalize to python date + fallback future detection
    signup = []
    for raw, ts in zip(df["signup_date"], signup_pd):
        if pd.isna(ts):
            # fallback: detect absurd future years
            try:
                year = int(str(raw)[:4])
                if year > today.year + 1:
                    signup.append(date(year, 1, 1))
                    continue
            except Exception:
                pass
            signup.append(None)
        else:
            signup.append(ts.date())

    dob = []
    for ts in dob_pd:
        if pd.isna(ts):
            dob.append(None)
        else:
            dob.append(ts.date())

    # invalid formats
    anomalies["invalid_signup_format"] = sum(s is None for s in signup)
    anomalies["invalid_dob_format"] = sum(d is None for d in dob)

    # future signup
    future_mask = [(s is not None and s > today) for s in signup]
    future_signup = df[future_mask]
    anomalies["signup_future"] = len(future_signup)

    # before cutoff
    pre_mask = [(s is not None and s < cutoff) for s in signup]
    pre_cutoff = df[pre_mask]
    anomalies["signup_before_2018"] = len(pre_cutoff)

    return ValidationResult(
        "anomalies",
        passed=(
            len(invalid_emails) == 0 and
            len(future_signup) == 0 and
            len(pre_cutoff) == 0 and
            completeness["null_customer_id"] == 0
        ),
        metrics={
            "invalid_email": len(invalid_emails),
            "signup_future": len(future_signup),
            "signup_before_2018": len(pre_cutoff),
            "null_customer_id": completeness["null_customer_id"],
        },
        details=anomalies
    )
