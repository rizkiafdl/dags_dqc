from .context import Context

def run_dag(tasks, deps, context:Context):
    upstreams = deps
    downstreams = {t: [] for t in tasks}

    for child, parents in deps.items():
        for p in parents:
            downstreams[p].append(child)

    pending = set(tasks)
    while pending:
        runnable = [t for t in pending if all(up in context.results for up in upstreams.get(t, []))]

        for t in runnable:
            # SOURCE: no upstream
            if not upstreams.get(t):
                context.results[t] = tasks[t](context)
            # SINK: no downstream
            elif not downstreams.get(t):
                context.results[t] = tasks[t](context)
            # MIDDLE: has both
            else:
                context.results[t] = tasks[t](context)

            pending.remove(t)

    return context.results
