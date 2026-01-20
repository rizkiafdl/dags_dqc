from dags.orchestrator import run_dag
from dags.definition import TASKS, DEPS
from dags.context import Context

from utils.loader import load_csv

def main():
    df = load_csv("data/customer.csv")
    ctx = Context(df=df)
    
    run_dag(TASKS, DEPS, ctx)

if __name__ == "__main__":
    main()