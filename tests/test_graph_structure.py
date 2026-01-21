import pytest
from dags.orchestrator import run_dag

def test_orphan_detection(ctx):
    TASKS = {"A": lambda c: None, "B": lambda c: None}
    DEPS = {"B": ["A"]}  

    TASKS["C"] = lambda c: None  

    with pytest.raises(Exception):
        run_dag(TASKS, DEPS, ctx)

def test_cycle_detection(ctx):
    TASKS = {"A": lambda c: None, "B": lambda c: None}
    DEPS = {"A":["B"], "B":["A"]}

    with pytest.raises(Exception):
        run_dag(TASKS, DEPS, ctx)

def test_missing_dependency(ctx):
    TASKS = {"A": lambda c: None}
    DEPS = {"A": ["B"]}  # B does not exist

    with pytest.raises(Exception):
        run_dag(TASKS, DEPS, ctx)
