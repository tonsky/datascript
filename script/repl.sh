#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

echo "Starting Socket REPL server on port 5555"
clj -A:1.11.1:dev:test:bench -X clojure.core.server/start-server :name repl :port 5555 :accept clojure.core.server/repl :server-daemon false
