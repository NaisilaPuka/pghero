require "bundler/gem_tasks"
require "rake/testtask"

task default: :test
Rake::TestTask.new do |t|
  t.libs << "test"
  t.pattern = "test/**/*_test.rb"
end

task :test2
Rake::TestTask.new do |t|
  t.libs << "test"
  t.pattern = "test/**/*_test_citus.rb"
end