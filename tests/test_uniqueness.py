import pandas as pd
from dags.context import Context
from tasks.uniqueness import uniqueness_check

def test_uniqueness_pass(base_row):
    df = pd.DataFrame([base_row])
    res = uniqueness_check(Context(df))
    assert bool(res.passed) is True
    assert res.metrics["duplicate_key_count"] == 0
    assert res.metrics["null_customer_id_count"] == 0

def test_uniqueness_fail_duplicate(base_row):
    df = pd.DataFrame([base_row, base_row])
    res = uniqueness_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["duplicate_key_count"] == 1

def test_uniqueness_fail_null(base_row):
    base_row["customer_id"] = None
    df = pd.DataFrame([base_row])
    res = uniqueness_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["null_customer_id_count"] == 1

def test_uniqueness_conflict(base_row):
    v1 = base_row.copy()
    v2 = base_row.copy()
    v2["first_name"] = "Different"
    df = pd.DataFrame([v1, v2])
    res = uniqueness_check(Context(df))
    assert bool(res.passed) is False
    assert res.metrics["conflict_records_count"] == 2
