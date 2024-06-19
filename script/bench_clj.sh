#!/bin/bash
cd "`dirname $0`/.."

clojure -A:bench -M -m datascript.bench.datascript $@