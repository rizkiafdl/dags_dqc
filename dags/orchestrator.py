from .context import Context

def detect_orphans(tasks, deps):
    referenced = set(deps.keys()) | {p for parents in deps.values() for p in parents}
    orphans = set(tasks) - referenced
    return orphans

def run_dag(tasks, deps, context: Context):
    # detect orphan tasks
    orphans = detect_orphans(tasks, deps)
    if orphans:
        raise Exception(f"Orphan task(s) detected: {', '.join(orphans)}")

    upstreams = deps
    downstreams = {t: [] for t in tasks}

    for child, parents in deps.items():
        for p in parents:
            downstreams[p].append(child)

    pending = set(tasks)
    while pending:
        runnable = [t for t in pending if all(up in context.results for up in upstreams.get(t, []))]
        if not runnable:
            raise Exception("Cycle or unresolved deps")

        for t in runnable:
            context.results[t] = tasks[t](context)
            pending.remove(t)

    return context.results