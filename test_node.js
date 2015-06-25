#!/usr/local/bin/node

var fs = require('fs');

global.goog = {};

global.CLOSURE_IMPORT_SCRIPT = function(src) {
  require('./target/none/goog/' + src);
  return true;
};

function nodeGlobalRequire(file) {
  process.binding('evals').NodeScript.runInThisContext.call(global, fs.readFileSync(file), file);
}

if (fs.existsSync("./target/none")) {
  nodeGlobalRequire('./target/none/goog/base.js');
  nodeGlobalRequire('./target/none/cljs_deps.js');
  goog.require('datascript.test');
} else {
  nodeGlobalRequire('./target/datascript.js');
}

if ("--all" === process.argv[2])
  var res = datascript.test.test_all();
else if ("--btset" === process.argv[2])
  var res = datascript.test.test_btset();
else
  var res = datascript.test.test_most();

if (res.fail + res.error > 0)
  process.exit(1);
