from dags.orchestrator import run_dag

def test_topological_execution_order(ctx):
    order = []

    TASKS = {
        "A": lambda c: order.append("A"),
        "B": lambda c: order.append("B"),
        "C": lambda c: order.append("C"),
        "D": lambda c: order.append("D"),
    }

    DEPS = {
        "B": ["A"],
        "C": ["A"],
        "D": ["B", "C"],
    }

    run_dag(TASKS, DEPS, ctx)

    assert order[0] == "A"
    assert order[-1] == "D"
    assert set(order) == {"A","B","C","D"}
