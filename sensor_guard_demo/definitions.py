from dagster import Definitions, load_assets_from_modules

from sensor_guard_demo import assets
from sensor_guard_demo.sensors import (
    always_failing_sensor,
    decay_recovery_sensor,
    flaky_sensor,
    multi_table_sensor,
    recovers_after_3_sensor,
    recovers_after_failure_sensor,
    refresh_data_job,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[refresh_data_job],
    sensors=[
        always_failing_sensor,
        decay_recovery_sensor,
        flaky_sensor,
        multi_table_sensor,
        recovers_after_3_sensor,
        recovers_after_failure_sensor,
    ],
)
