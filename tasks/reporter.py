from dags.results import ValidationResult

def report(context):
    print("\n==== DQC REPORT ====\n")

    passed_map = {}
    aggregated_metrics = {}
    aggregated_details = {}

    for name, res in context.results.items():
        if name == "report":
            continue
        
        print(f"[{name.upper()}]")
        print(f"  passed: {res.passed}")
        print(f"  metrics: {res.metrics}")
        if res.details:
            print(f"  details: {res.details}")
        print()

        passed_map[name] = bool(res.passed)
        aggregated_metrics[name] = res.metrics
        aggregated_details[name] = res.details

    final_passed = all(passed_map.values()) if passed_map else True

    result = ValidationResult(
        "report",
        final_passed,
        metrics=aggregated_metrics,
        details=aggregated_details,
    )

    context.results["report"] = result
    return result
