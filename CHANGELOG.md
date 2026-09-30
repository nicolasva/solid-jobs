# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- `Server#stop` is bounded: components get `shutdown_timeout + STOP_GRACE`
  to return, `:stop` is re-sent up to `STOP_RESENDS` times, then the
  component is abandoned with an error log instead of hanging shutdown.
- `test/support/ractor_barrier_repro.rb`: standalone reproducer of the Ruby
  3.4 Ractor GC-barrier deadlock (no SolidJobs, no Redis).

### Changed

- Server-based stress tests are skipped on Ruby < 4: Ruby 3.4's Ractor
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

- Ractor-oriented client, Processor, Scheduler, Heartbeat, and Server runtime.
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

[Unreleased]: https://github.com/nicolasva/solid-jobs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nicolasva/solid-jobs/releases/tag/v0.1.0
