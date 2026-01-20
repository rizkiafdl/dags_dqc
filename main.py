from utils.loader import load_csv
from dags.orchestrator import run_dag
from dags.DAG import TASKS, DEPS

def main():
    df = load_csv("data/customer.csv")
    run_dag(TASKS, DEPS, df)

if __name__ == "__main__":
    main()