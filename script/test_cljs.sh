#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

lein with-profile test cljsbuild once advanced
node script/test_cljs.js