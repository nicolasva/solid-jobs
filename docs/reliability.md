# Reliability model

SolidJobs uses a namespaced Redis-backed at-least-once state machine:

```text
READY
  |
  | atomic claim
  v
CLAIMED ---- fenced completion ----> removed
  |
  +---- application error ----> RETRYING or DISCARDED, then completion
  +---- graceful timeout -----> READY
  +---- node crash -----------> claim retained
                                  |
                                  +---- recovery ----> READY
```

Ready tasks live in `solid_jobs:channel:<name>`. Each executor owns at most one
claimed-task list and one claim journal entry:

```text
task_id      stable across replay
claim_token  unique per execution attempt
node_id      owning server identity
executor_id  owning Executor Ractor
channel      destination used by recovery
attempt      monotonically increasing execution count
claimed_at   wall-clock claim time
```

A ready payload has a short-lived publication barrier while its
`job.enqueued` and `job.journaled` events are emitted. Executors leave marked
payloads in READY until the publisher removes the marker. Markers expire after
30 seconds, so a publisher crash can delay a task but cannot strand it.

Completion and requeue are fenced by `claim_token`. Their Lua scripts verify
that the executor still owns the current journal generation before changing
state. A delayed or revived executor cannot complete or requeue a newer claim.

The envelope retains its canonical `channel`, allowing another node to restore
it after its owner dies. Recovery uses node liveness and heartbeat, never claim
age alone, so a legitimate long-running task is not stolen.

## Failure boundaries

- Before claim: the task remains in `solid_jobs:channel:<name>`.
- After claim or during `execute_task`: the task remains claimed.
- After the application effect but before completion: recovery replays it.
- Redis unavailable during completion: the task remains claimed and is replayed.
- Graceful shutdown: the active task may finish within the configured timeout;
  otherwise it is interrupted and requeued.
- `SIGKILL`: recovery relies exclusively on Redis state.

This design favors no task loss over duplicate suppression. Exactly-once side
effects require application-level idempotency.

## Startup isolation

The server does not claim work while components are booting. Heartbeat,
Engine, and Timer Ractors initialize their local configuration and Redis
pool, report `READY` exactly once, and wait behind
`SolidJobs::StartupBarrier`:

```text
BOOTING -> ALL_READY -> RUNNING
    \-------> BOOT_FAILED -> cleanup
```

Boot failure is terminal. Already-ready components receive `:abort`, close
their local resources, and never enter their claim loops. Cleanup is
idempotent.

## Ruby 3.4 Ractor caveat

Ruby 3.4's Ractor scheduler can deadlock the VM when a GC-triggered scheduler
barrier runs while several Ractors exchange moved messages.
`test/support/ractor_barrier_repro.rb` reproduces this without SolidJobs or
Redis. Ruby 4.0.1 completes the equivalent reproducer.

Run multi-Ractor SolidJobs servers on Ruby >= 4.0. On Ruby 3.4, prefer one
process per Engine (`concurrency: 1`). Conductor stress tests are skipped on
Ruby versions affected by this runtime issue, and `Conductor#start` warns when it
detects a multi-Ractor configuration there.

## Bounded shutdown

`Conductor#stop` never waits forever for a component. Each Ractor gets
`shutdown_timeout + Conductor::STOP_GRACE` to return after `:stop`; the signal is
re-sent up to `STOP_RESENDS` times before the component is abandoned with an
error log.

## Configuration scope

`SolidJobs.config` and the testing mode are Ractor-local, not thread-local.
Every thread and fiber inside a Ractor shares its configuration, while each
Ractor owns isolated mutable state and Redis connections.

## Integrity auditing

Fault tests can reconcile known task IDs against every Redis-backed state:

```ruby
report = SolidJobs::IntegrityCheck.call(
  expected_job_ids: submitted_task_ids,
  acked_key: "test:completed",
)

raise report.inspect unless report.ok?
```

The report separates lost tasks, unexpected tasks, inconsistent claim
journals, dangling attempt indexes, duplicate active states, and malformed
envelopes. Completion removes the task's attempt index atomically; requeue
retains it so recovery increments the same attempt sequence.

## Backpressure and channels

Each Engine owns at most one claim and claims only immediately before
execution. A node with concurrency `N` therefore holds at most `N` active
claims, regardless of channel depth.

Channel order defaults to `:weighted`:

- `:weighted` uses bounded weighted round-robin;
- `:priority` checks channels in declaration order;
- `:shuffle` samples the configured weighted channel list.

Paused channels are excluded before claiming.

## Retry storms

Retries use capped exponential backoff with equal jitter. For failure `n`, the
ceiling is `min(retry_base_delay * 2**(n - 1), retry_max_delay)` and the delay
is distributed between half and all of that ceiling. Defaults are 15 seconds
and one hour.
