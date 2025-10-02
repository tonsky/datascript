#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

clojure -X:1.12:dev:bench:datomic clojure+.core.server/start-server
