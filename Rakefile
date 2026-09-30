# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new do |task|
  task.libs << "lib"
  task.libs << "test"
  task.test_files = FileList["test/**/*_test.rb"]
    .exclude(
      "test/stress/**/*_test.rb",
      "test/stress_long_test.rb",
      "test/soak_test.rb",
    )
  task.warning = true
end

Rake::TestTask.new(:stress) do |task|
  task.libs << "lib"
  task.libs << "test"
  task.test_files = FileList["test/stress_test.rb", "test/stress/**/*_test.rb"]
    .exclude(
      "test/stress/torture_test.rb",
      # Native-crash reproducer for concurrent Ractor/RESP startup on Ruby 3.4;
      # it has its own explicit `startup_torture` task.
      "test/stress/resp_reader_startup_test.rb",
    )
  task.warning = true
end

Rake::TestTask.new(:torture) do |task|
  task.libs << "lib"
  task.libs << "test"
  task.test_files = FileList["test/stress/torture_test.rb"]
  task.warning = true
end

Rake::TestTask.new(:startup_torture) do |task|
  task.libs << "lib"
  task.libs << "test"
  task.test_files = FileList["test/stress/resp_reader_startup_test.rb"]
  task.warning = true
end

Rake::TestTask.new(:soak) do |task|
  task.libs << "lib"
  task.libs << "test"
  task.test_files = FileList["test/soak_test.rb"]
  task.warning = true
end

namespace :benchmark do
  desc "Profile SolidJobs reserve, deserialize, dispatch, metrics, and ACK overhead"
  task :hot_path do
    ruby "benchmark/hot_path.rb"
  end

  desc "Compare pure Ractor CPU scaling with SolidJobs in-memory dispatch"
  task :cpu_scaling do
    ruby "benchmark/cpu_scaling.rb"
  end
end

task default: :test
