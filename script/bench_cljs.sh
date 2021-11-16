#!/bin/bash
cd "`dirname $0`/.."

lein with-profile +bench cljsbuild once bench
node script/bench_cljs.js $@