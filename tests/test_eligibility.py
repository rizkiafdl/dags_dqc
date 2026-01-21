from tasks.eligibility import eligibility_check
from dags.context import Context
import pandas as pd

def test_eligibility_pass():
    df = pd.DataFrame([
        {"customer_id":"1004","date_of_birth":"1990-01-01", "user_status":"T"}
    ])
    res = eligibility_check(Context(df))
    assert res.passed is True

def test_eligibility_fail():
    df = pd.DataFrame([
        {"customer_id":"1002","date_of_birth":"2010-01-01", "user_status":"F"}
    ])
    res = eligibility_check(Context(df))
    assert res.passed is False
