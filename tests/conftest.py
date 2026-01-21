import pytest
import pandas as pd
from dags.context import Context

@pytest.fixture
def base_row():
    return {
        "customer_id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "date_of_birth": "1990-01-01",
        "signup_date": "2023-01-01",
        "user_status": "T",
    }

@pytest.fixture
def ctx(base_row):
    df = pd.DataFrame([base_row])
    return Context(df)
