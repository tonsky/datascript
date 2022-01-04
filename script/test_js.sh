#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

lein with-profile test cljsbuild once release
node test_node.js --js