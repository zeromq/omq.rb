#!/bin/sh
set -e
export GEM_PATH="$SNAP/lib/ruby/gems/4.0.0"
# Classic confinement inherits the host LD_LIBRARY_PATH; prepend our bundled
# libs so we resolve libruby / libssl / libyaml / libsodium from the snap and
# not from whatever the host happens to ship. Ruby itself lives under $SNAP/
# (effective prefix `/`); stage-packages from debs live under $SNAP/usr/lib.
export LD_LIBRARY_PATH="$SNAP/lib:$SNAP/usr/lib/$SNAP_ARCH_TRIPLET:${LD_LIBRARY_PATH:-}"
exec "$SNAP/bin/ruby" --yjit "$SNAP/lib/ruby/gems/4.0.0/bin/omq" "$@"
