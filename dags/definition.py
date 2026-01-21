from tasks.uniqueness import uniqueness_check
from tasks.eligibility import eligibility_check
from tasks.anomalies import anomalies_check
from tasks.reporter import report

TASKS = {
    "uniqueness": uniqueness_check,
    "eligibility": eligibility_check,
    "anomalies_check":anomalies_check,
    "report": report
}

# uniqueness -> eligibility -> anomalies_check -> report
DEPS = {
    "eligibility": ["uniqueness"],
    "anomalies_check" :["eligibility"],
    "report": ["anomalies_check"],
}

