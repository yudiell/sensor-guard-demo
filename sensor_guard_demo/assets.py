import random

from dagster import asset


@asset
def raw_data():
    """Simulates fetching raw data from an external source."""
    rows = [{"id": i, "value": random.randint(1, 100)} for i in range(10)]
    return rows


@asset(deps=[raw_data])
def processed_data():
    """Simulates processing the raw data."""
    return {"status": "processed", "record_count": 10}
