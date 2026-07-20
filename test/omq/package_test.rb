# frozen_string_literal: true

require "minitest/autorun"

class PackageTest < Minitest::Test
  ROOT = File.expand_path("../..", __dir__)

  def test_main_gem_does_not_package_subgems
    spec = Gem::Specification.load(File.join(ROOT, "omq.gemspec"))
    leaked = spec.files.grep(%r{\Agems/})

    assert_empty leaked
  end
end
