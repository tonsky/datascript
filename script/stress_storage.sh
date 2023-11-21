#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

clojure -J-Xmx512m -M:test -m datascript.test.storage $@