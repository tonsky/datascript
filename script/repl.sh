#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

clj -A:test:bench -J--add-opens=java.base/java.io=ALL-UNNAMED -X clojure.core.server/start-server :name repl :port 5555 :accept clojure.core.server/repl :server-daemon false
