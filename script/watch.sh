#!/bin/zsh -euo pipefail

cd "`dirname $0`/.."

clojure -A:test -M -m kaocha.runner --watch "$@"