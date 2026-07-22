# frozen_string_literal: true

require "minitest/autorun"

class PackageTest < Minitest::Test
  ROOT = File.expand_path("../..", __dir__)

  def test_main_gem_does_not_package_subgems
    spec = Gem::Specification.load(File.join(ROOT, "omq.gemspec"))
    leaked = spec.files.grep(%r{\Agems/})

    assert_empty leaked
  end


  def test_subgems_do_not_package_build_outputs
    Dir[File.join(ROOT, "gems/*/*.gemspec")].each do |gemspec|
      spec = load_gemspec_from_own_directory(gemspec)
      leaked = spec.files.grep(%r{\A(?:tmp|target|pkg)/|\A\.\./|\A/})

      assert_empty leaked, "#{spec.name} packages #{leaked.inspect}"
    end
  end


  private


  def load_gemspec_from_own_directory(path)
    Dir.chdir(File.dirname(path)) do
      Gem::Specification.load(File.basename(path))
    end
  end
end
