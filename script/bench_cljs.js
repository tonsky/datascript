#! node

global.performance = {
  now: function () {
         var t = process.hrtime();
         return t[0] * 1000 + t[1] / 1000000;
       }
}

require("../target/datascript.js");

datascript.bench.datascript._main(...process.argv.slice(2));