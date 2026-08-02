# omq Benchmarks

Measured on Linux x86_64, Ruby 4.0.2 +YJIT (io_uring).

## Throughput (PUSH/PULL, piping `seq N`)

```
               stdin
                 |
+----------------v-----------------------+
| seq N | omq push -c ipc://             |
+----------------+-----------------------+
                 | IPC/TCP
+----------------v-----------------------+
| omq pull -b ipc:// > /dev/null         |
+----------------------------------------+
```

| Transport | msg/s |
|-----------|-------|
| tcp | 19k |
| ipc | 20k |

## Latency (REQ/REP, `-D "ping" -i 0`)

```
+--------------------------------------+
| omq req -c ipc:// -D ping -i 0       |
+-----------------+--------------------+
                  |  req/rep
+-----------------v--------------------+
| omq rep -b ipc:// --echo             |
+--------------------------------------+
```

| Transport | µs/roundtrip |
|-----------|-------------|
| tcp | 657 |
| ipc | 643 |

## Running

```sh
sh bench/cli/throughput.sh [count]                    # default: 10000
sh bench/cli/latency.sh [count]                       # default: 1000
sh bench/cli/fib_pipeline/pipeline.sh [count]         # default: 1000
sh bench/cli/fib_pipeline/pipeline_ractors.sh [count] # default: 1000
```

See also: [fib_pipeline/](fib_pipeline/) for pipeline benchmark results.
