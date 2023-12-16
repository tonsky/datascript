#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

clj -A:1.11.1:dev:test:bench -M -m user
