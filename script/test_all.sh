#!/bin/bash

lein test-clj
lein with-profile test cljsbuild once advanced
node test_node.js --all
