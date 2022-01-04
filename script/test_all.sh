#!/bin/bash
set -o nounset -o pipefail
cd "`dirname $0`/.."

./script/test_clj.sh
./script/test_cljs.sh
./script/test_js.sh
./script/test_datomic.sh