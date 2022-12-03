#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

clj -A:test:bench:datomic:nrepl -M -m nrepl.cmdline --interactive