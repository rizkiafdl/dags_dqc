def run_dag(tasks, deps, df):
    completed = {}
    pending = set(tasks)

    while pending:
        runnable = [t for t in pending if all(d in completed for d in deps.get(t, []))]

        for t in runnable:
            if deps.get(t):
                completed[t] = tasks[t](completed)
            else:
                completed[t] = tasks[t](df)
            pending.remove(t)

    return completed
