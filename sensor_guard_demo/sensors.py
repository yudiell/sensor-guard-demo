import random

from dagster import (
    AssetKey,
    RunRequest,
    SkipReason,
    define_asset_job,
    sensor,
)
from dagster_sensor_guard import resilient_sensor

# Job that materializes both assets.
refresh_data_job = define_asset_job(
    name="refresh_data_job",
    selection=[AssetKey("raw_data"), AssetKey("processed_data")],
)


# ---------------------------------------------------------------------------
# Sensor 1: Always fails — demonstrates error suppression then threshold breach
# ---------------------------------------------------------------------------
@sensor(job=refresh_data_job, minimum_interval_seconds=30)
@resilient_sensor(threshold=3)
def always_failing_sensor(context):
    """This sensor always raises to demonstrate suppression.

    Errors 1-3 will be suppressed with a SkipReason.
    Error 4 will raise to Dagster and show as a sensor failure in the UI.
    A manual reset of the sensor (toggle off/on) resets the cursor & counter.
    """
    raise ConnectionError("Simulated external service unreachable")


# ---------------------------------------------------------------------------
# Sensor 2: Escalating flaky pattern — demonstrates decay reset under pressure
# ---------------------------------------------------------------------------
# F=fail, S=succeed: F,S, FF,S, F,S, FFF,S, FFFF,S
_flaky_script = [
    False, True,                          # fail 1, succeed
    False, False, True,                   # fail 2, succeed
    False, True,                          # fail 1, succeed
    False, False, False, True,            # fail 3, succeed
    False, False, False, False, True,     # fail 4 (breaches threshold), succeed
]
_flaky_tick = 0  # module-level counter (resets when dagster dev restarts)


@sensor(job=refresh_data_job, minimum_interval_seconds=5)
@resilient_sensor(threshold=3, reset_strategy="decay", decay_amount=1)
def flaky_sensor(context):
    """Escalating flaky pattern that eventually breaches the threshold.

    - Uses decay reset: each success subtracts 1 from the error count
      instead of clearing it, so the sensor must sustain multiple successes
      to fully recover.
    - With threshold=3 the sensor tolerates up to 3 consecutive errors.
    - The pattern builds up: 1 fail, 2 fails, 1 fail, 3 fails, then 4 fails
      which breaches the threshold on the 4th consecutive error.
    """
    global _flaky_tick  # noqa: PLW0603
    tick = _flaky_tick
    _flaky_tick += 1

    # Past the script? Always succeed.
    if tick < len(_flaky_script) and not _flaky_script[tick]:
        raise ConnectionError("Simulated intermittent timeout")

    yield RunRequest(run_key=f"flaky-{context.cursor or 0}")
    context.update_cursor(str(int(context.cursor or 0) + 1))


# ---------------------------------------------------------------------------
# Sensor 3: Fails a few times then recovers — demonstrates full reset
# ---------------------------------------------------------------------------
_countdown = 3  # module-level counter (resets when dagster dev restarts)


@sensor(job=refresh_data_job, minimum_interval_seconds=30)
@resilient_sensor(threshold=3)
def recovers_after_3_sensor(context):
    """Fails the first 3 ticks, then succeeds.

    Demonstrates that a single success resets the error count to 0
    (full reset strategy, the default).
    """
    global _countdown  # noqa: PLW0603
    if _countdown > 0:
        _countdown -= 1
        raise RuntimeError(f"Service warming up ({_countdown} failures left)")

    yield SkipReason("Service is healthy, nothing to process")


# ---------------------------------------------------------------------------
# Sensor 4: Fails, breaches, recovers, fails again, recovers — full lifecycle
# ---------------------------------------------------------------------------
# Scripted sequence: fail, fail, fail, fail, succeed, fail, succeed, ...
_recovery_script = [False, False, False, False, True, False, True]
_recovery_tick = 0  # module-level counter (resets when dagster dev restarts)


@sensor(job=refresh_data_job, minimum_interval_seconds=10)
@resilient_sensor(threshold=3)
def recovers_after_failure_sensor(context):
    """Follows a scripted fail/succeed pattern to demonstrate the full lifecycle.

    Tick 1-3: Errors suppressed (1/3, 2/3, 3/3).
    Tick 4:   Threshold breached — error raised to Dagster.
    Tick 5:   Recovers — launches RunRequest, counter resets.
    Tick 6:   Fails again — suppressed (1/3), proving counter reset.
    Tick 7+:  Recovers and continues normally.
    """
    global _recovery_tick  # noqa: PLW0603
    tick = _recovery_tick
    _recovery_tick += 1

    # Past the script? Always succeed.
    if tick < len(_recovery_script) and not _recovery_script[tick]:
        raise RuntimeError(f"Service unavailable (tick {tick + 1})")

    yield RunRequest(run_key=f"recovery-{context.cursor or 0}")
    context.update_cursor(str(int(context.cursor or 0) + 1))
