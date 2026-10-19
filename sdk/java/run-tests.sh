#!/usr/bin/env bash
# Compiles with plain javac (JDK 11+) and runs the conformance tests against the fake server.
set -euo pipefail
cd "$(dirname "$0")"
out="$(mktemp -d)"; trap 'rm -rf "$out"' EXIT
javac -Xlint:all -d "$out" $(find src -name '*.java')
../conformance/run.sh java -cp "$out" com.cumulus13.msmqcache.ConformanceTest
