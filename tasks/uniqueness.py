from dags.results import ValidationResult
import pandas as pd

IDENTITY_FIELDS = ["first_name", "last_name", "date_of_birth", "email"]

def uniqueness_check(context):
    df = context.df

    uniqueness_violation = {}
    #Null PK values break "uniqueness"
    null_ids = df[df["customer_id"].isna()]
    uniqueness_violation["null_customer_id"] = null_ids.to_dict(orient="records")

    #Duplicate identity keys
    dup_keys = (
        df[df.duplicated(subset=["customer_id"], keep=False)]["customer_id"]
        .dropna()
        .unique()
        .tolist()
    )

    dup_rows = df[df["customer_id"].isin(dup_keys)].to_dict(orient="records")
    uniqueness_violation["duplicate_primary_keys"] = dup_keys
    uniqueness_violation["duplicate_primary_keys_rows"] = dup_rows

    # Identity conflicts (same PK, different identity attributes)
    conflicts = []
    for cid in dup_keys:
        subset = df[df["customer_id"] == cid]
        distinct = subset[IDENTITY_FIELDS].drop_duplicates()
        if len(distinct) > 1:
            conflicts.append({
                "customer_id": cid,
                "variants": distinct.to_dict(orient="records")
            })
    
    variant_count = sum(len(c["variants"]) for c in conflicts)
    
    uniqueness_violation["identity_conflicts"] = conflicts

    passed = (
        len(null_ids) == 0 and
        len(dup_keys) == 0 and
        variant_count == 0
    )
  
    return ValidationResult(
        "uniqueness",
        passed=passed,
        metrics={
            "null_customer_id_count": len(null_ids),
            "duplicate_key_count": len(dup_keys),
            "conflict_records_count": variant_count,
        },
        details=uniqueness_violation
    )
