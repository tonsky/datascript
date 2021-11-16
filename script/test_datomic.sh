#!/bin/zsh -euo pipefail
cd "`dirname $0`/.."

clj -A:datomic -M -m test-datomic.pull-api