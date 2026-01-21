import pandas as pd
from dags.context import Context
from dags.orchestrator import run_dag
from dags.definition import TASKS, DEPS

def test_end_to_end_df(base_row):
    bad = base_row.copy()
    bad["email"] = "broken"
    bad["signup_date"] = "9999-01-01"
    bad["user_status"] = "F"
    df = pd.DataFrame([bad])
    ctx = Context(df)
    results = run_dag(TASKS, DEPS, ctx)
    assert "report" in results
    assert bool(results["report"].passed) is False
