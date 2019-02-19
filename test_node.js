#!/usr/local/bin/node

var fs = require('fs'),
    vm = require('vm');

global.performance = { now: function () {
  var t = process.hrtime();
  return t[0] * 1000 + t[1] / 1000000;
} }

global.goog = {};

global.CLOSURE_IMPORT_SCRIPT = function(src) {
  require('./target/none/goog/' + src);
  return true;
};

function nodeGlobalRequire(file) {
  vm.runInThisContext.call(global, fs.readFileSync(file), file);
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
  var tests_js = require('./test/js/tests.js');
}

if ("--all" === process.argv[2])
  var res = merge(datascript.test.test_cljs(), tests_js.test_all());
else if ("--js" === process.argv[2])
  var res = tests_js.test_all();
else if ("--cljs" === process.argv[2])
  var res = datascript.test.test_cljs();
else {
  var t0 = global.performance.now();
  var res = datascript.test.test_most();
  var t1 = global.performance.now();
  console.log("Time: " + (t1-t0) + "msec");
}

if (res.fail + res.error > 0)
  process.exit(1);
