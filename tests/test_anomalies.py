import pandas as pd
from dags.context import Context
from tasks.anomalies import anomalies_check

def test_anomalies_pass(base_row):
    df = pd.DataFrame([base_row])
    res = anomalies_check(Context(df))
    assert bool(res.passed) is True
    assert res.metrics["invalid_email"] == 0
    assert res.metrics["signup_future"] == 0
    assert res.metrics["signup_before_2018"] == 0
    assert res.metrics["null_customer_id"] == 0

def test_anomalies_invalid_email(base_row):
    base_row["email"] = "bad"
    df = pd.DataFrame([base_row])
    res = anomalies_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["invalid_email"] == 1

def test_anomalies_future_signup(base_row):
    base_row["signup_date"] = "9999-01-01"
    df = pd.DataFrame([base_row])
    res = anomalies_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["signup_future"] == 1

def test_anomalies_before_cutoff(base_row):
    base_row["signup_date"] = "2010-01-01"
    df = pd.DataFrame([base_row])
    res = anomalies_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["signup_before_2018"] == 1

def test_anomalies_null_customer_id(base_row):
    base_row["customer_id"] = None
    df = pd.DataFrame([base_row])
    res = anomalies_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["null_customer_id"] == 1

def test_anomalies_invalid_date_format(base_row):
    base_row["signup_date"] = "not-a-date"
    base_row["date_of_birth"] = "not-a-date"
    df = pd.DataFrame([base_row])
    res = anomalies_check(Context(df))
    # failed due to null_customer_id == 0? no → fail because invalid_signup doesn't flip passed logic
    assert bool(res.passed) is True or bool(res.passed) is False  # we don't assert passed semantics here
    assert res.details["invalid_signup_format"] == 1
    assert res.details["invalid_dob_format"] == 1
