var page = require('webpage').create();

page.onConsoleMessage = function (message) {
    console.log(message);
};

page.open('test/js/js.html', function (status) {
    console.log("\nTesting test.datascript.js\n");
    var result = page.evaluate(function() {
        return run_tests();
    });
  
    phantom.exit(result);
});
