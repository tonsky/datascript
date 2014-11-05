#!/bin/sh

set -e

(cat release/wrapper.prefix; cat release/datascript.bare.js; cat release/wrapper.suffix) > release/datascript.js

echo "Packed release/datascript.js"
