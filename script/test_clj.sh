#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

lein with-profile test run -m datascript.test/test-clj