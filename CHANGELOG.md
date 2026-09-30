# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
