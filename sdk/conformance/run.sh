#!/usr/bin/env bash
# Starts two fake servers (open + password "s3cret") and runs the given command with
#   MCACHE_TEST_PORT / MCACHE_TEST_AUTH_PORT / MCACHE_TEST_PASSWORD set.
# usage: sdk/conformance/run.sh <command...>

set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"
python3 "$here/fake_server.py" > "$tmp/open.out" &  p1=$!
python3 "$here/fake_server.py" --password s3cret > "$tmp/auth.out" &  p2=$!
trap 'kill $p1 $p2 2>/dev/null || true; rm -rf "$tmp"' EXIT
for i in $(seq 1 50); do
  grep -q PORT "$tmp/open.out" 2>/dev/null && grep -q PORT "$tmp/auth.out" 2>/dev/null && break
  sleep 0.1
done
export MCACHE_TEST_PORT="$(awk '{print $2}' "$tmp/open.out")"
export MCACHE_TEST_AUTH_PORT="$(awk '{print $2}' "$tmp/auth.out")"
export MCACHE_TEST_PASSWORD=s3cret
"$@"
