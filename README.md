# SolidJobs

[![Build Status](https://github.com/nicolasva/solid-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/nicolasva/solid-jobs/actions/workflows/ci.yml)
[![Code Climate](https://codeclimate.com/github/nicolasva/solid-jobs/badges/gpa.svg)](https://codeclimate.com/github/nicolasva/solid-jobs)
[![Gem Version](https://badge.fury.io/rb/solid-jobs.svg)](https://rubygems.org/gems/solid-jobs)
[![Documentation Status](https://img.shields.io/badge/docs-rubydoc.info-blue.svg)](https://www.rubydoc.info/gems/solid-jobs)
[![Downloads](https://img.shields.io/gem/dt/solid-jobs.svg)](https://rubygems.org/gems/solid-jobs)

SolidJobs is a Ractor-oriented Redis background job system for Ruby. It uses
`solid-redis` for Redis access and keeps mutable clients, pools, middleware,
and runtime state local to their owning Ractor.

SolidJobs is an independent implementation. It does not depend on or load the
Sidekiq gem. Its Redis job payloads and queue keys are designed to be
compatible with the Sidekiq 8 open-source data format.

SolidJobs and its required gems use pure Ruby and do not require native
extensions. Hot paths are designed around bounded buffers, reusable immutable
configuration, and low-allocation command batches.

## Installation

Add SolidJobs 0.1 to your bundle:

```ruby
gem "solid-jobs", "~> 0.1.0"
```

Then run:

```sh
bundle install
```

## Delivery semantics

SolidJobs provides **at-least-once** job delivery. A worker atomically moves a
job from `queue:<name>` to a process reservation list before execution and
removes it only after a successful ACK. Graceful shutdown requeues unfinished
jobs; reservations owned by a crashed process are recovered into their
original queues.

A process can still crash after the application side effect and before the
ACK. The recovered job will then run again. Jobs must therefore be idempotent
or implement an application-level deduplication key when duplicate side
effects are unsafe. SolidJobs does not claim exactly-once execution.

```ruby
class HardJob
  include SolidJobs::Job

  solid_jobs_options queue: "critical", retry: 10

  def perform(account_id)
    Account.find(account_id).recalculate!
  end
end

HardJob.perform_async(42)
HardJob.perform_in(30, 42)
```

Run workers:

```sh
bundle exec solid-jobs --require ./config/environment \
  --concurrency 8 --queue critical,3 --queue default
```

## Tests

SolidJobs uses Minitest exclusively:

```sh
# Unit and bounded Redis integration tests
bundle exec rake test

# Bounded concurrency and multi-process recovery stress
STRESS_JOBS=10000 bundle exec rake stress

# Reproducible random-fault campaign
SOLID_JOBS_TORTURE=1 STRESS_JOBS=100000 bundle exec rake torture

# Re-run an exact failure sequence
SOLID_JOBS_TORTURE=1 STRESS_JOBS=100000 STRESS_SEED=123456 bundle exec rake torture

# Repeated fresh-process RESP/Ractor startup torture
STARTUP_TORTURE_CYCLES=1000 \
STARTUP_TORTURE_READERS=100 \
bundle exec rake startup_torture

# Long-running stability; defaults to 24 hours
SOLID_JOBS_SOAK=1 SOLID_JOBS_SOAK_SECONDS=86400 bundle exec rake soak
```

The torture report reconciles enqueued, uniquely completed, duplicate,
dead, queued, and reserved jobs. Any non-zero `LOST` value fails the test.

Profile the reliable execution path independently from application work:

```sh
REDIS_URL=redis://127.0.0.1:6379/0 \
HOT_PATH_JOBS=10000 \
bundle exec rake benchmark:hot_path
```

The report separates time and allocations for reservation, payload reuse,
observability registration, dispatch/perform wrapping, observability cleanup,
and fenced ACK. Fetch decodes the payload once for both reservation metadata
and dispatch; its job body is intentionally empty.

Diagnose CPU scaling independently from Redis:

```sh
CPU_SCALING_JOBS=1000 \
CPU_SCALING_ITERATIONS=210000 \
bundle exec rake benchmark:cpu_scaling
```

This runs the identical CPU loop through pure Ractors and through the
SolidJobs in-memory dispatch path. Each 1/2/4/8 case uses fresh processes and
reports execution latency, scaling efficiency, CPU-seconds per 1,000 jobs,
RSS, allocations, GC time, heap slots, and malloc growth.

## Sidekiq comparison

The separate `benchmark_sidekiq_solid-jobs` bundle runs Sidekiq and SolidJobs
against the same isolated Redis server. It covers enqueue, bulk enqueue,
CPU-bound processing, I/O-bound processing, mixed processing, and long-running
stability. Every measurement runs in a fresh Ruby process; client order
alternates and the default report uses the median of six repetitions.

The current homogeneous CPU-processing reference uses Ruby 4.0.1,
Sidekiq 8.1.7, SolidJobs 0.1.2, a 100,000-iteration integer workload, and
2,000 jobs per case:

| Concurrency | Sidekiq jobs/s | SolidJobs jobs/s | SolidJobs scaling | Sidekiq CPU-s/1k | SolidJobs CPU-s/1k | Sidekiq RSS | SolidJobs RSS | Sidekiq alloc/job | SolidJobs alloc/job |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 289 | 281 | 100.0% | 3.35 | 3.40 | 41.4 MiB | 81.8 MiB | 117.2 | 84.0 |
| 2 | 296 | 549 | 97.7% | 3.38 | 3.44 | 41.7 MiB | 83.1 MiB | 116.8 | 79.7 |
| 4 | 298 | 1,090 | 97.0% | 3.37 | 3.43 | 41.8 MiB | 79.5 MiB | 111.1 | 77.7 |
| 8 | 298 | 2,053 | 91.4% | 3.36 | 3.67 | 42.4 MiB | 74.1 MiB | 108.1 | 74.3 |

At eight concurrency units, SolidJobs reaches 2,053 jobs/s versus 298 jobs/s
for Sidekiq. This is Ractor parallelism rather than equal CPU efficiency:
SolidJobs consumes 752.6% CPU and 3.67 CPU-seconds per 1,000 jobs, while
Sidekiq consumes 100.2% CPU and 3.36 CPU-seconds per 1,000 jobs. SolidJobs
also uses more RSS, but reaches 27.71 jobs/s/MiB versus 7.03 for Sidekiq.

These are local synthetic measurements, not application-capacity claims.
Queue p95/p99 values in this run use sparse sampling and are excluded from the
summary until the final latency campaign increases the sample count. Ruby
3.4.4 eight-Ractor results are also excluded: Ruby 3.4's Ractor scheduler
crashed (concurrent TCP/RESP initialization, reproduced on macOS arm64 and
Linux x86_64) or deadlocked VM-wide on a GC barrier under cross-Ractor
message traffic (`test/support/ractor_barrier_repro.rb` reproduces it without
SolidJobs).
Ruby 4.0.1 passed the equivalent reproducers, and `StartupBarrier` serializes
component initialization before releasing normal parallel processing.
**Multi-Ractor servers are recommended on Ruby ≥ 4.0**; see
`docs/reliability.md`.

The project is under active development. The Web UI and commercial Sidekiq
features are not part of the initial scope.
