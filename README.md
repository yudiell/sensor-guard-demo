# sensor-guard-demo

Demo project for [dagster-sensor-guard](https://github.com/yudiell/dagster-sensor-guard) — a decorator that adds automatic error suppression, thresholds, and recovery strategies to Dagster sensors.

## Quick start

```bash
uv run dagster dev
```

Open [localhost:3000](http://localhost:3000) and enable the sensors to watch them cycle through failures, suppression, breaches, and recovery.

## Sensors

| # | Sensor | What it demonstrates |
|---|--------|---------------------|
| 1 | `always_failing_sensor` | Every tick raises. Errors are suppressed up to the threshold (3), then the 4th error is raised to Dagster. |
| 2 | `flaky_sensor` | Escalating fail/succeed pattern with `decay` reset. Each success only subtracts 1 from the counter, so recovery is gradual. Eventually breaches after 4 consecutive failures. |
| 3 | `recovers_after_3_sensor` | Fails the first 3 ticks then succeeds. Shows that a single success resets the counter to 0 (full reset, the default). |
| 4 | `recovers_after_failure_sensor` | Scripted fail/succeed lifecycle: suppression → breach → recovery → re-suppression. Proves the counter resets after a breach. |
| 5 | `decay_recovery_sensor` | Heavy error accumulation with `decay` reset (`decay_amount=1`). Needs many consecutive successes to fully recover from a long failure streak. |
| 6 | `multi_table_sensor` | **Per-key failure tracking** (`per_key=True`). Tracks 3 tables independently in a single sensor — see details below. |

## Sensor 6: Per-key failure tracking

`multi_table_sensor` demonstrates the `per_key=True` feature, which gives each resource its own independent error counter within a single sensor.

Three simulated tables:
- **`orders`** — always healthy, yields a `RunRequest` every tick
- **`customers`** — flaky, fails the first 2 ticks then recovers
- **`inventory`** — persistently broken, fails every tick until it breaches

### Tick-by-tick behavior

| Tick | orders | customers | inventory |
|------|--------|-----------|-----------|
| 1 | ok | fail (1/3) | fail (1/3) |
| 2 | ok | fail (2/3) | fail (2/3) |
| 3 | ok | ok (counter resets) | fail (3/3) |
| 4 | ok | ok | fail → breach (`SensorGuardKeyError`) |
| 5+ | ok | ok | ok |

### What to look for in the Dagster UI

- **Ticks 1-3**: The sensor completes successfully. `orders` produces `RunRequest`s while `customers` and `inventory` errors are silently suppressed.
- **Tick 4**: The sensor tick shows a failure — `SensorGuardKeyError` reports that `inventory` exceeded the threshold. `orders` and `customers` still produced their `RunRequest`s on that same tick.
- **Tick 5+**: All three tables are healthy, and the sensor produces `RunRequest`s for all of them.
