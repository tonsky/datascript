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
  goog.require('datascript.js');
} else {
  nodeGlobalRequire('./target/datascript.js');
}

function merge(m1, m2) {
  return { fail:  m1.fail  + m2.fail,
           error: m1.error + m2.error,
           test:  m1.test  + m2.test,
           pass:  m1.pass  + m2.pass }
}

if ("--all" === process.argv[2] || "--js" === process.argv[2]) {
  nodeGlobalRequire('./test/js/underscore-1.6.0.min.js');
  nodeGlobalRequire('./test/js/tests.js');
}

if ("--all" === process.argv[2])
  var res = merge(datascript.test.test_all(), test_datascript_js());
else if ("--btset" === process.argv[2])
  var res = datascript.test.test_btset();
else if ("--js" === process.argv[2]) {
  var res = test_datascript_js();
} else
  var res = datascript.test.test_most();

if (res.fail + res.error > 0)
  process.exit(1);

