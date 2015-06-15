#!/usr/local/bin/node

test = "bench_" + (process.argv[2] || "all");
path = process.argv[3] || "./target/datascript.js";

require(path);
datascript.bench[test]();
