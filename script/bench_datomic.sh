#!/bin/bash
cd "`dirname $0`/.."

clj -A:bench:datomic -M -m datascript.bench.datomic $@