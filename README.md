## **Overview**

This project implements a minimal Data Quality Check (DQC) pipeline that runs over a customer dataset. The checks are modeled as DAG tasks and include:

* **Uniqueness validation** on primary keys
* **Eligibility validation** (business rules)
* **Anomaly detection** (completeness + semantic checks)
* **Reporting** of results

The execution graph ensures tasks run in the correct order and fail if dependency contracts break.

## **Running the Pipeline**

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Execute the pipeline:

```bash
python main.py
```

This loads `data/customer.csv`, runs all tasks, and prints a validation report.

---

## **Running Tests**

Unit tests are provided. Run from project root:

```bash
pytest -v
```

Tests cover execution order, dependency graph behavior, task results, and end-to-end validation.

---

## **Dataset Source**

`data/customer.csv` 

---

## **Validation Logic**

### **1. Uniqueness Rules**

Primary key: `customer_id`

We check:

* duplicate keys
* null keys
* conflicts (same key, mismatched attributes)

Output includes:

```
duplicate_count
duplicate_primary_keys
conflict_count
```

### **2. Eligibility Rules**

A user is eligible if:

```
age > 17 AND user_status == "T"
```

Returns list of invalid users (either under-aged or inactive).

### **3. Anomaly Detection**

Anomalies include:

* completeness issues (nulls)
* invalid email format
* invalid or future signup dates
* unreasonable historical dates (< 2018)

Metrics include counts for each anomaly type.

## **Answers to Provided Questions**

> **1. Define a set of rules to check for uniqueness and return number of duplicated rows**

Primary key = `customer_id`
Uniqueness rule checks:

* count of duplicate keys
* list of records with affected keys
* structural integrity (null keys)

> **2. Eligibility rules for DANA**

Eligible user must:

* be older than 17 years
* have `user_status == "T"`

Invalid users = those failing either condition.

> **3. What anomalies appear in the dataset and how should it ideally appear**

Detected anomalies:

* missing names (`first_name`, `last_name`)
* invalid email formats (`example`, missing `@`)
* future signup dates (`9999-01-01`)

Ideal data would:

* have complete customer identity
* use valid email format
* use realistic signup dates within system lifetime
* enforce consistent typing across rows

## **Directory Structure**

```
.
├── dags/              # pipeline orchestration
├── tasks/             # validation logic
├── data/              # customer.csv input
├── tests/             # pytest suite
├── utils/             # loader utilities
├── requirements.txt
├── main.py
└── README.md
```