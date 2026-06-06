
#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."
PATH="$PWD/.fvm/flutter_sdk/bin:$PATH"

rm -rf tmp/cljdbench
mkdir -p tmp/cljdbench/src/cljd
cat > tmp/cljdbench/deps.edn <<EOF
{:paths ["src" "../../bench"]
 :deps {org.clojure/clojure {:mvn/version "1.11.1"}
        tensegritics/clojuredart
        {:git/url "https://github.com/tensegritics/ClojureDart.git"
         :sha "1a8c70ab4e8901a4c68ed4507f43966e9337b5fb"}
        io.github.wevre/transit-cljd {:git/url "https://github.com/Roam-Research/transit-cljd.git"
                                      :sha "9d4511f0ef50705641b084f432bab726c64a8832"}
        datascript/datascript {:local/root "../../"}}
 :cljd/opts {:main datascript.bench.datascript
             :kind :dart}}
EOF

cd tmp/cljdbench
clojure -M -m cljd.build init
clojure -M -m cljd.build compile datascript.bench.datascript
dart run bin/cljdbench.dart $@
