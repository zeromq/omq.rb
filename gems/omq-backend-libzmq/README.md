# omq-backend-libzmq

[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/omq-backend-libzmq?color=e9573f)](https://rubygems.org/gems/omq-backend-libzmq)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%204.0-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

Use libzmq under the same OMQ socket API. Requires libzmq 4.x installed
on the system.

```ruby
require "omq"
require "omq/backend/libzmq"

push = OMQ::PUSH.new(backend: :libzmq)
push.connect("tcp://127.0.0.1:5555")
push.send("hello from libzmq")
```

The libzmq backend replaces the pure Ruby ZMTP stack with libzmq. The
socket API, options, and Async integration remain identical. A dedicated
I/O thread per socket handles all libzmq operations because libzmq
sockets are not thread-safe.

`require "omq/ffi"` and `backend: :ffi` remain supported for compatibility.

## Interop

libzmq and native Ruby backends are wire-compatible. You can mix them
freely:

```ruby
# native REP server
rep = OMQ::REP.bind("tcp://127.0.0.1:5555")

# libzmq REQ client
req = OMQ::REQ.new(backend: :libzmq)
req.connect("tcp://127.0.0.1:5555")
```

## Requirements

- Ruby >= 4.0
- libzmq 4.x (`libzmq5` / `libzmq3-dev` on Debian/Ubuntu)
- [omq](https://github.com/zeromq/omq.rb) >= 0.28
