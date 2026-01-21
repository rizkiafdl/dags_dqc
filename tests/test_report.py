from dags.context import Context
from dags.results import ValidationResult
from tasks.reporter import report
import pandas as pd

def test_report_collects_results(base_row):
    ctx = Context(pd.DataFrame([base_row]))
    ctx.results = {
        "uniqueness": ValidationResult("uniqueness", False, {}, {}),
        "eligibility": ValidationResult("eligibility", True, {}, {}),
        "anomalies": ValidationResult("anomalies", False, {}, {}),
    }
    res = report(ctx)
    assert isinstance(res, ValidationResult)
    assert bool(res.passed) is False
