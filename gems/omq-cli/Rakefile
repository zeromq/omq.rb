# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test" << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

desc "Run omq CLI system tests"
task "test:system" do
  sh "sh test/system/run_all.sh"
end

task default: :test
