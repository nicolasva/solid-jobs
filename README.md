# SolidJobs

[![Build Status](https://github.com/nicolasva/solid-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/nicolasva/solid-jobs/actions/workflows/ci.yml)
[![Gem Version](https://badge.fury.io/rb/solid-jobs.svg)](https://rubygems.org/gems/solid-jobs)
[![Downloads](https://img.shields.io/gem/dt/solid-jobs?style=flat)](https://rubygems.org/gems/solid-jobs)
[![Documentation Status](https://img.shields.io/badge/docs-RubyDoc.info-blue.svg)](https://www.rubydoc.info/gems/solid-jobs)

SolidJobs is a Ractor-oriented Redis task runner for Ruby. It provides its own
task API, Redis envelope, keyspace, interceptor model, failure policy, and
inspection API.

SolidJobs is **not compatible with Sidekiq**. It does not read Sidekiq queues,
does not write Sidekiq payloads, and does not expose Sidekiq's job API. Existing
applications and queued work must be migrated explicitly.

## Installation

```ruby
gem "solid-jobs"
```

Then run `bundle install`.

## Defining and submitting tasks

```ruby
class RecalculateAccount
  include SolidJobs::Task

  task_options channel: "critical", max_failures: 10

  def execute_task(account_id)
    Account.find(account_id).recalculate!
  end
end

RecalculateAccount.enqueue(42)
RecalculateAccount.enqueue_after(30, 42)
RecalculateAccount.schedule_at(Time.now + 300, 42)
RecalculateAccount.enqueue_many([[42], [43], [44]])
```

The persisted envelope is specific to SolidJobs:

```json
{
  "id": "7ec33ed5-1b77-4bde-8960-bd77231f10ef",
  "task": "RecalculateAccount",
  "arguments": [42],
  "channel": "critical",
  "max_failures": 10,
  "created_ms": 1790981000000,
  "queued_ms": 1790981000001
}
```

All internal Redis keys use the `solid_jobs:` namespace. Ready work is stored
under `solid_jobs:channel:<name>`; planned, retrying, discarded, node, claim,
attempt, and metric data use separate namespaced keys.

Run executors:

```sh
bundle exec solid-jobs --require ./config/environment \
  --concurrency 8 --channel critical,3 --channel default
```

## Configuration

```ruby
SolidJobs.configure do |config|
  config.channels = [["critical", 3], "default"]
  config.channel_order = :weighted
  config.concurrency = 8
  config.shutdown_timeout = 25
  config.default_task_options = {
    channel: "default",
    max_failures: 25,
  }
end
```

`channel_order` accepts `:weighted` for bounded weighted round-robin,
`:priority` for declaration-order priority, and `:shuffle` for random
selection from the weighted channel list.

## Interceptors

Publication and execution use SolidJobs interceptors. An interceptor implements
`around(context)` and yields to continue:

```ruby
class TraceExecution
  def around(context)
    Telemetry.start(context.envelope.fetch("id"))
    yield
  ensure
    Telemetry.finish
  end
end

SolidJobs.configure do |config|
  config.execute_interceptors.use(TraceExecution)
end
```

Publication interceptors receive `SolidJobs::Publisher::Publication`; execution
interceptors receive `SolidJobs::Executor::Execution`.

## Failure handling

Tasks default to 25 failures. A task can customize the delay or final action:

```ruby
class ImportCatalog
  include SolidJobs::Task

  task_options max_failures: 8, retry_within: 3_600

  retry_delay do |failure_count, error, envelope|
    :drop if error.is_a?(InvalidCatalog)
  end

  after_final_failure do |envelope, error|
    Alerts.catalog_import_failed(envelope.fetch("id"), error)
  end
end
```

`retry_delay` may return a delay in seconds, `:drop`, or `:archive`. Without an
override, SolidJobs uses capped exponential backoff with equal jitter.

## Delivery semantics

SolidJobs provides **at-least-once** delivery. An executor atomically claims a
task before execution and completes the claim only after `execute_task` returns.
Graceful shutdown requeues unfinished tasks. Claims owned by a crashed node are
recovered into their original channels.

A process can still crash after an application side effect and before claim
completion. The recovered task then runs again. Tasks must be idempotent or use
application-level deduplication when duplicate effects are unsafe.

See [docs/reliability.md](docs/reliability.md) for the state machine, claim
fencing, recovery rules, and Ruby Ractor caveats.

## Node control

```ruby
node = SolidJobs::Nodes.new.first
node.request_control(:pause)
node.request_control(:backtraces)
node.request_control(:shutdown)
```

Control requests are asynchronous and use the selected node's namespaced Redis
mailbox. Unknown actions raise `KeyError` without writing a request.

## Lab

```ruby
SolidJobs.testing!(:capture) do
  RecalculateAccount.enqueue(42)
  RecalculateAccount.captured
end

SolidJobs.testing!(:execute) do
  RecalculateAccount.enqueue(42)
end
```

Run the project suites:

```sh
bundle exec rake test
STRESS_JOBS=10000 bundle exec rake stress
SOLID_JOBS_TORTURE=1 STRESS_JOBS=100000 bundle exec rake torture
SOLID_JOBS_SOAK=1 SOLID_JOBS_SOAK_SECONDS=86400 bundle exec rake soak
```

## Migrating from Sidekiq

There is no transparent migration path because compatibility is intentionally
absent:

1. Replace `include Sidekiq::Job` with `include SolidJobs::Task`.
2. Replace `sidekiq_options` with `task_options`.
3. Replace `perform_async`, `perform_in`, and `perform_bulk` with `enqueue`,
   `enqueue_after`, and `enqueue_many`.
4. Replace middleware with SolidJobs interceptors.
5. Drain or export existing Sidekiq queues before switching. SolidJobs will not
   consume them.
6. Start SolidJobs with `--channel`; `--queue` is not accepted.

The Active Job adapter remains available as
`ActiveJob::QueueAdapters::SolidJobsAdapter`, but it writes only SolidJobs
envelopes and keys.

## Instrumentation

SolidJobs exposes an optional Ractor-local instrumenter and defaults to a
no-op. Any configured instrumenter must respond to `instrument(name, payload)`
and be Ractor-shareable before a conductor starts. Ordinary instrumentation
failures are logged when possible and never change publication, execution,
retry, acknowledgement, recovery, or shutdown behavior. Process-control
interruptions still propagate so shutdown can requeue in-flight work.

SolidJobs emits the v1 lifecycle events `job.enqueued`, `job.journaled`,
`job.reserved`, `job.started`, `job.completed`, `job.failed`,
`job.retry_scheduled`, `job.dead`, `job.acknowledged`, and `job.recovered`.
Ready jobs are not reservable until `job.enqueued` and `job.journaled` have
been emitted; an expiring Redis publication barrier preserves this order
without stranding work if a publisher exits.
They carry correlation identifiers and structured errors, but never task
arguments, business payloads, secrets, or textual logs. `node_id` is shared by
all Ractors in a conductor; `ractor_id` and `worker_id` are integer processor
identities. `reservation_id` is the reliable claim token.

The heartbeat Ractor also samples and emits `process.observed`, one
`ractor.observed` per logical processor, and one `redis.observed` per processor
every five seconds. Process observations are limited to process CPU time, RSS,
GC count/time, and total Ruby allocations. Sources are local Ruby/OS reads made
outside worker Ractors. A source that has never succeeded is `unavailable`; a
later failure preserves the last value as `stale` with its increasing age.
SolidJobs never estimates CPU, memory, GC, or allocations per Ractor.

Ractor observations expose only state, activity, integer
`ractor_id`/`worker_id`/`executor_id`, and current job IDs. Quiet workers report
`waiting`; shutdown reports `stopping` then `stopped` without inventing work.
Redis connection state comes from worker operations already being performed:
successful operations report `connected` and `SolidRedis::ConnectionError`
reports `disconnected`. No monitoring-only Redis command is added. Redis
latency, command, memory, and connection-count metrics remain explicitly
`unavailable` until a reliable source exists.

## Performance: SolidJobs vs Sidekiq

SolidJobs uses Ruby Ractors for parallel execution across real CPU cores, while
Sidekiq uses threads within a single Ruby process subject to the Global VM
Lock (GVL).

The benchmark suite (`benchmark_sidekiq_solid-jobs`) measures throughput and
resource usage under identical workloads against an isolated Redis server.

### CPU Workload (`process-cpu`)

Sidekiq remains capped by Ruby's GVL near 1 core (~100% CPU), whereas SolidJobs
scales linearly across cores:

| Concurrency | Sidekiq (jobs/s) | SolidJobs (jobs/s) | SolidJobs Scaling | Sidekiq CPU | SolidJobs CPU |
|---:|---:|---:|---:|---:|---:|
| 1 | 290 | 282 | 100.0% | 95.9% | 94.6% |
| 2 | 299 | 556 | 98.5% | 100.3% | 190.7% |
| 4 | 303 | 1,094 | 96.9% | 100.3% | 378.2% |
| 8 | 302 | **2,170** | **96.2%** | 100.1% | **761.0%** |

*At 8 concurrency, SolidJobs delivers **7.2× the throughput** of Sidekiq on pure CPU workloads.*

### Mixed Workload (`process-mixed` — CPU + I/O)

| Concurrency | Sidekiq (jobs/s) | SolidJobs (jobs/s) | SolidJobs Scaling |
|---:|---:|---:|---:|
| 1 | 178 | 195 | 100.0% |
| 2 | 402 | 419 | 107.4% |
| 4 | 567 | 822 | 105.4% |
| 8 | 578 | **1,816** | **116.3%** |

*At 8 concurrency, SolidJobs delivers **3.1× the throughput** of Sidekiq on mixed workloads.*

### I/O Workload (`process-io`)

| Concurrency | Sidekiq (jobs/s) | SolidJobs (jobs/s) | SolidJobs Allocations/job |
|---:|---:|---:|---:|
| 1 | 151 | **158** | 58 |
| 2 | 302 | **311** | 54 |
| 4 | 593 | **604** | 50 |
| 8 | 1,121 | **1,133** | 52 |

### Client Enqueue Throughput

| Concurrency | Sidekiq Enqueue (jobs/s) | SolidJobs Enqueue (jobs/s) | Sidekiq Bulk (jobs/s) | SolidJobs Bulk (jobs/s) |
|---:|---:|---:|---:|---:|
| 1 | 22,497 | 24,125 | 249,694 | 204,206 |
| 2 | 24,691 | 43,201 | 286,079 | 354,652 |
| 4 | 24,137 | 66,750 | 291,407 | 617,298 |
| 8 | 23,462 | **90,956** | 285,176 | **852,909** |

Compared with SolidJobs 0.4.0 on the same benchmark matrix, individual
enqueue throughput improved by 2.2–4.3%. Bulk throughput at 8 concurrency
improved by 3.5%. CPU scaling efficiency at 8 Ractors improved from 94.0% to
96.2%.

### Memory & Allocations

- **Peak RSS:** Comparable processing footprint at 8 concurrency (42.5 MiB for
  SolidJobs and 41.9 MiB for Sidekiq on CPU tasks).
- **Allocations:** SolidJobs produces fewer object allocations per task (48 vs
  108 allocations/job on CPU tasks, 41 vs 60 on individual enqueue), reducing
  GC pressure. CPU-task allocations fell by 36% from SolidJobs 0.4.0.

*Environment: Ruby 4.0.1 (arm64-darwin25); Sidekiq 8.1.7; SolidJobs 0.4.1.
Fresh process per measurement; medians of six runs; zero benchmark errors.*
