#!/bin/bash
cd "`dirname $0`/.."

./script/bench_clj.sh $@
./script/bench_cljs.sh $@
./script/bench_datomic.sh $@