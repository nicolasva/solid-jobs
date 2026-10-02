# SolidJobs

[![Build Status](https://github.com/nicolasva/solid-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/nicolasva/solid-jobs/actions/workflows/ci.yml)
[![Gem Version](https://badge.fury.io/rb/solid-jobs.svg)](https://rubygems.org/gems/solid-jobs)

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
