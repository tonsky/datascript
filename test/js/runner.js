var page = require('webpage').create();

page.onConsoleMessage = function (message) {
    console.log(message);
};

page.open('test/js/tests.html', function (status) {
    console.log("\nTesting datascript.js\n");
    var result = page.evaluate(function() {
        return run_tests();
    });
  
    phantom.exit(result);
});
