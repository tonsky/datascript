#!/bin/bash
set -o nounset -o pipefail
cd "`dirname $0`/.."

EXIT=0
./script/test_clj.sh || EXIT=$((EXIT + $?))
./script/test_cljs.sh || EXIT=$((EXIT + $?))
./script/test_js.sh || EXIT=$((EXIT + $?))
./script/test_datomic.sh || EXIT=$((EXIT + $?))
exit $EXIT