# Changelog

All notable changes to this project will be documented in this file.

## [0.4.1] - 2026-10-04

### Changed

- Reduce allocations in reliable claims and task dispatch.
- Require SolidRedis 1.0.12 or newer.

## [0.4.0] - 2026-10-03

### Changed

- Keep the independent SolidJobs runtime, envelope, `solid_jobs:` keyspace,
  interceptors, claims, and renamed component vocabulary from 0.3.0.
- Remaining shared names with other Ruby job libraries are limited to
  `version.rb`, Rails Active Job adapter contracts (`ActiveJob`,
  `QueueAdapters`, `enqueue_at`), and generic Ruby methods.

## [0.3.0] - 2026-10-03

### Breaking

- Rename the runtime components to SolidJobs-specific names:
  `Config` to `Blueprint`, `Server` to `Conductor`, `Processor` to `Engine`,
  `CLI` to `Console`, `Testing` to `Lab`, and `Metrics` to `Counters`.
- Rename the task execution callback from `perform` to `execute_task`.
- Rename `Task.enqueue_at` to `Task.schedule_at`. The Active Job adapter keeps
  its Rails-required `enqueue_at` method.
- Rename the Active Job execution task from `ActiveJob::Wrapper` to
  `RailsAdapter::AdapterTask`.
- Rename generic implementation files to match the new component vocabulary.
  `version.rb` remains unchanged as the standard Ruby gem version entry point.
- Remove the remaining generic internal constants `ClassMethods`,
  `EmptyQueueError`, and `Shutdown` in favor of `TaskMethods`,
  `NoCapturedTask`, and `ExecutionHalt`.
- Replace `Node#quiet`, `Node#stop`, and `Node#dump_threads` with
  `Node#request_control(:pause)`, `:shutdown`, or `:backtraces`.

### Changed

- Update the executable, Rails adapter, documentation, benchmarks, and test
  suite to use the new vocabulary consistently.
- Keep the SolidJobs task API, envelope, `solid_jobs:` Redis keyspace, claims,
  interceptors, and failure model introduced in 0.2.0.
- Convert configuration keys using Ruby hash transformations, preserving
  nested values and leaving the original input unchanged.

## [0.2.0] - 2026-10-03

### Breaking

- Replace the previous job API with the independent `SolidJobs::Task` API:
  `enqueue`, `enqueue_after`, `schedule_at`, `enqueue_many`, `execute`, and
  `with_options`.
- Replace the previous payload with a SolidJobs envelope using `id`, `task`,
  `arguments`, `channel`, `run_at`, `created_ms`, and `queued_ms`.
- Move all Redis data into the `solid_jobs:` keyspace. Existing queued data is
  not read or migrated automatically.
- Replace middleware chains with `publish_interceptors` and
  `execute_interceptors`, whose interceptors implement `around(context)`.
- Replace the administrative API with `Counters`, `Channel`, `StoredTask`,
  `PlannedTasks`, `RetryingTasks`, `DiscardedTasks`, `Node`, `Nodes`,
  `Execution`, and `Claims`.
- Replace queue configuration with channels and the `:weighted`, `:priority`,
  and `:shuffle` ordering modes.
- Remove all Sidekiq compatibility aliases and wire-format compatibility.

### Changed

- Claims use UUID task IDs and claim tokens, with generation-fenced completion
  and requeue operations.
- Failure handling uses `max_failures`, `retry_within`, `retry_delay`, and
  `after_final_failure`.
- Lab modes are now `:capture` and `:execute`.

## [0.1.3] - 2026-10-01

### Changed

- Require `solid-redis` 1.0.11 and rely on its `solid-resp-ractor` dependency
  instead of declaring and loading the RESP codec directly.

## [0.1.2] - 2026-10-01

### Added

- `Conductor#start` logs a warning on Ruby < 4 when `concurrency > 1`, pointing
  to the Ruby 3.4 Ractor GC-barrier deadlock and the recommended setups.
- `Conductor#stop` is bounded: components get `shutdown_timeout + STOP_GRACE`
  to return, `:stop` is re-sent up to `STOP_RESENDS` times, then the
  component is abandoned with an error log instead of hanging shutdown.
- `test/support/ractor_barrier_repro.rb`: standalone reproducer of the Ruby
  3.4 Ractor GC-barrier deadlock (no SolidJobs, no Redis).

### Changed

- Conductor-based stress tests are skipped on Ruby < 4: Ruby 3.4's Ractor
  scheduler deadlocks the VM on a GC barrier under cross-Ractor `move:`
  traffic. Multi-Ractor servers are recommended on Ruby ≥ 4.0 (see
  `docs/reliability.md`). CI jobs now time out after 20 minutes.

## [0.1.1] - 2026-10-01

### Fixed

- `SolidJobs.config` and the testing mode are now Ractor-local instead of
  thread-local. Jobs enqueued from threads spawned after configuration (Puma
  workers, Rails request threads) no longer fall back to a default
  `localhost:6379` Redis.

### Changed

- `rake stress` no longer runs the Ruby 3.4 concurrent-startup crash
  reproducer; use `rake startup_torture` explicitly.

## [0.1.0] - 2026-09-30

### Added

- Ractor-oriented client, Engine, Scheduler, Heartbeat, and Conductor runtime.
- Sidekiq-compatible Redis queue keys and open-source job payload format.
- Immediate, scheduled, retry, and dead-job handling.
- At-least-once reservations with atomic journaling, generation-fenced ACK
  and requeue operations, and crashed-process recovery.
- `SolidJobs::IntegrityCheck` for lost jobs, orphaned references, invalid
  reservations, dangling indexes, and impossible duplicate states.
- `SolidJobs::StartupBarrier`, which preconnects every component before
  releasing Processors into their fetch loops.
- Graceful quiet and shutdown handling with requeue of interrupted work.
- Minitest unit, Redis integration, bounded stress, torture, startup-torture,
  and soak suites.
- Reliable-hot-path and CPU-scaling profilers plus the independent Sidekiq
  comparison benchmark.

[0.3.0]: https://github.com/nicolasva/solid-jobs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nicolasva/solid-jobs/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/nicolasva/solid-jobs/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/nicolasva/solid-jobs/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/nicolasva/solid-jobs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/nicolasva/solid-jobs/releases/tag/v0.1.0
