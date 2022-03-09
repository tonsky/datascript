#! node

var fs = require('fs'),
    vm = require('vm');

global.performance = { now: function () {
  var t = process.hrtime();
  return t[0] * 1000 + t[1] / 1000000;
} }

global.goog = {};

function nodeGlobalRequire(file) {
  vm.runInThisContext.call(global, fs.readFileSync(file), file);
}

nodeGlobalRequire('./release-js/datascript.js');

var tests_js = require('../test/js/tests.js');
var res = tests_js.test_all();

if (res.fail + res.error > 0)
  process.exit(1);
