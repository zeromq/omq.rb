# frozen_string_literal: true

require "bundler/gem_tasks"
Bundler::GemHelper.tag_prefix = "omq-"

require "rake/testtask"

Rake::TestTask.new("test:omq") do |t|
  t.libs << "test" << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

def gem_test_task(name, min_ruby: nil)
  Rake::TestTask.new("test:#{name}") do |t|
    t.libs << "gems/#{name}/test" << "gems/#{name}/lib" << "test" << "lib"
    t.test_files = FileList["gems/#{name}/test/**/*_test.rb"]
  end

  return unless min_ruby

  original = Rake::Task["test:#{name}"].actions.dup
  Rake::Task["test:#{name}"].clear_actions
  Rake::Task["test:#{name}"].enhance do
    if Gem::Version.new(RUBY_VERSION) < Gem::Version.new(min_ruby)
      warn "Skipping #{name}: Ruby #{min_ruby}+ required"
    else
      original.each { |action| action.call(Rake::Task["test:#{name}"]) }
    end
  end
end

gem_test_task "omq-backend-libzmq", min_ruby: "4.0"
gem_test_task "omq-lz4", min_ruby: "4.0"
gem_test_task "omq-qos"
gem_test_task "omq-ractor", min_ruby: "4.0"
gem_test_task "omq-websocket"
gem_test_task "omq-zstd", min_ruby: "4.0"

task "test:omq-backend-rust" do
  sh "bundle", "exec", "rake", "-C", "gems/omq-backend-rust"
end

task test: [
  "test:omq",
  "test:omq-backend-libzmq",
  "test:omq-backend-rust",
  "test:omq-lz4",
  "test:omq-qos",
  "test:omq-ractor",
  "test:omq-websocket",
  "test:omq-zstd",
]

task default: :test
