#!/bin/bash

lein test-clj
lein cljsbuild once advanced release
node test_node.js --all
