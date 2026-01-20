def report(results):
    print("\n==== DQC REPORT ====\n")
    for name, res in results.items():
        if name == "report":
            continue
        print(f"[{name.upper()}]")
        print(f"  passed: {res.passed}")
        print(f"  metrics: {res.metrics}")
        if res.details:
            print(f"  details: {res.details}")
        print()
