#! node

global.performance = {
  now: function () {
         var t = process.hrtime();
         return t[0] * 1000 + t[1] / 1000000;
       }
}

require("../target/datascript.js");

var res = datascript.test.test_cljs();

if (res.fail + res.error > 0)
  process.exit(1);
