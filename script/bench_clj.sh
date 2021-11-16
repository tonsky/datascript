#!/bin/bash
cd "`dirname $0`/.."

clj -A:bench -M -m datascript.bench.datascript $@