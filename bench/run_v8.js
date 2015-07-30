#!/usr/local/bin/node

global.performance = { now: function () {
  var t = process.hrtime();
  return t[0] * 1000 + t[1] / 1000000;
} }

test = "bench_" + (process.argv[2] || "all");
path = process.argv[3] || "./target/datascript.js";

require(path);
datascript.bench[test]();
