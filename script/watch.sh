#!/bin/zsh -euo pipefail

cd "`dirname $0`/.."

clojure -A:test -m kaocha.runner --watch "$@"