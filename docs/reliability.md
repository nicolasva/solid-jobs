# Reliability model

SolidJobs uses a Redis-backed at-least-once state machine:

```text
READY
  |
  | BLMOVE reservation
  v
IN_PROGRESS ---- ACK/LREM ----> removed
  |
  +---- application error ----> RETRY or DEAD, then ACK
  +---- graceful timeout -----> READY
  +---- process crash --------> reservation retained
                                  |
                                  +---- recovery ----> READY
```

The reservation list is named `<process-identity>:reserved:<processor-id>`.
Each execution receives a distinct reservation journal entry:

```text
job_id          stable across replay
reservation_id  unique per execution attempt
process_id      owning process identity
worker_id       owning Processor Ractor
attempt         monotonically increasing execution count
reserved_at     wall-clock reservation time
```

ACK and requeue are fenced by `reservation_id`. Their Lua scripts first
verify that the worker still owns the current journal generation. A delayed
or revived worker cannot remove or requeue a newer reservation, even when a
supervisor reuses the same processor slot.

The payload retains its canonical `queue` field, allowing another process to
restore it after the owner is no longer alive. Recovery uses process liveness
and heartbeat, never reservation age alone, so a legitimate long-running job
is not stolen while its owner remains alive.

## Failure boundaries

- Before reservation: the job remains in `queue:<name>`.
- After reservation or during `perform`: the job remains reserved.
- After the application effect but before ACK: recovery replays the job.
- Redis unavailable during ACK: the job remains reserved and is replayed.
- Graceful shutdown: the active job may finish within the configured timeout;
  otherwise it is interrupted and requeued.
- `SIGKILL`: no handler runs; recovery relies exclusively on Redis state.

This design intentionally favors no job loss over duplicate suppression.
Exactly-once side effects require application-level idempotency.

## Startup isolation

The server does not reserve work while components are booting. Heartbeat,
Processor, and Scheduler Ractors initialize their local configuration and
Redis pool, report `READY` exactly once, and wait behind
`SolidJobs::StartupBarrier`. Processing starts only after every component is
ready:

```text
BOOTING -> ALL_READY -> RUNNING
    \-------> BOOT_FAILED -> cleanup
```

Boot failure is terminal. Already-ready components receive `:abort`, close
their local resources, and never enter their fetch loops. Cleanup is
idempotent. Component startup is serialized, avoiding concurrent TCP/RESP
initialization paths known to crash Ruby 3.4.4 on the tested Apple Silicon
environment; normal processing remains parallel after `RUNNING`.

## Integrity auditing

Fault and chaos tests can reconcile a known set of job IDs against every
Redis-backed state:

```ruby
report = SolidJobs::IntegrityCheck.call(
  expected_job_ids: submitted_job_ids,
  acked_key: "test:completed",
)

raise report.inspect unless report.ok?
```

The report separates lost jobs, unexpected/orphaned jobs, inconsistent
reservation journals, dangling attempt indexes, duplicate active states, and
malformed payloads. ACK removes the completed job's attempt index atomically;
requeue retains it so a recovered reservation increments the same attempt
sequence.

## Backpressure and queues

Each Processor owns at most one reservation and reserves only immediately
before execution. A process with concurrency `N` therefore holds at most `N`
active reservations, regardless of Redis queue depth.

Queue mode defaults to `:weighted`:

- `:weighted` uses bounded weighted round-robin and does not starve configured
  queues;
- `:strict` always checks queues in declaration order and may intentionally
  starve lower-priority queues while a higher-priority queue remains busy;
- `:random` samples the configured weighted queue list on each reservation.

Paused queues are excluded before reservation.

## Retry storms

Retries use capped exponential backoff with equal jitter. For attempt `n`, the
ceiling is `min(retry_base_delay * 2**n, retry_max_delay)` and the actual delay
is distributed between half and all of that ceiling. Defaults are 15 seconds
and one hour. This spreads recovery traffic after a shared external outage.
