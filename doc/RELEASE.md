# Release

Human runbook for release tags and RubyGems trusted publishers. CI does not
read this file.

Releases publish from GitHub Actions on prefixed tags.
The release workflow runs the same Ruby 3.3/4.0 test matrix before publishing.

| Gem | Tag prefix | Directory |
|---|---|---|
| `omq` | `omq-v` | `.` |
| `omq-backend-rust` | `omq-backend-rust-v` | `gems/omq-backend-rust` |
| `omq-backend-libzmq` | `omq-backend-libzmq-v` | `gems/omq-backend-libzmq` |
| `omq-lz4` | `omq-lz4-v` | `gems/omq-lz4` |
| `omq-qos` | `omq-qos-v` | `gems/omq-qos` |
| `omq-ractor` | `omq-ractor-v` | `gems/omq-ractor` |
| `omq-websocket` | `omq-websocket-v` | `gems/omq-websocket` |
| `omq-zstd` | `omq-zstd-v` | `gems/omq-zstd` |

Example: tag `omq-zstd-v0.4.2` publishes `gems/omq-zstd`.

RubyGems trusted publishers for every gem should point to:

| Field | Value |
|---|---|
| Owner | `zeromq` |
| Repository | `omq.rb` |
| Workflow | `release.yml` |
| Environment | `rubygems` |

The workflow verifies that the tag suffix matches the selected gemspec
version before publishing.
