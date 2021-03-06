(def version "1.0.7")

(defproject datascript (str version (System/getenv "DATASCRIPT_CLASSIFIER"))
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure       "1.10.2"   :scope "provided"]
    [org.clojure/clojurescript "1.10.520" :scope "provided"]
    [persistent-sorted-set     "0.1.2"]
  ]
  
  :plugins [
    [lein-cljsbuild "1.1.7"]
  ]
  
  :global-vars {
    *warn-on-reflection*   true
    *print-namespace-maps* false
;;     *unchecked-math* :warn-on-boxed
  }
  :jvm-opts ["-Xmx2g" "-server"]

  :aliases {"test-clj"     ["run" "-m" "datascript.test/test-clj"]
            "test-cljs"    ["do" ["cljsbuild" "once" "release" "advanced"]
                                 ["run" "-m" "datascript.test/test-node" "--all"]]
            "node-repl"    ["run" "-m" "user/node-repl"]
            "browser-repl" ["run" "-m" "user/browser-repl"]
            "test-all"     ["do" ["clean"] ["test-clj"] ["test-cljs"]]}
  
  :cljsbuild { 
    :builds [
      { :id "release"
        :source-paths ["src"]
        :assert false
        :compiler {
          :output-to     "release-js/datascript.bare.js"
          :optimizations :advanced
          :pretty-print  false
          :elide-asserts true
          :output-wrapper false 
          :parallel-build true
          :checked-arrays :warn
        }
        :notify-command ["release-js/wrap_bare.sh"]}
              
      { :id "advanced"
        :source-paths ["src" "test"]
        :compiler {
          :output-to     "target/datascript.js"
          :optimizations :advanced
          :source-map    "target/datascript.js.map"
          :pretty-print  true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
        }}

      { :id "bench"
        :source-paths ["src" "bench/src"]
        :compiler {
          :output-to     "target/datascript.js"
          :optimizations :advanced
          :source-map    "target/datascript.js.map"
          ; :pretty-print  true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
          ; :pseudo-names  true
          :fn-invoke-direct true
          :elide-asserts true
        }}

      { :id "none"
        :source-paths ["src" "test"]
        :compiler {
          :main          datascript.test
          :output-to     "target/datascript.js"
          :output-dir    "target/none"
          :optimizations :none
          :source-map    true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
        }}
  ]}

  :profiles {
    :dev { :source-paths ["test" "dev"]
           :dependencies [[org.clojure/tools.nrepl     "0.2.13"]
                          [org.clojure/tools.namespace "0.2.11"]
                          [lambdaisland/kaocha         "0.0-389"]
                          [lambdaisland/kaocha-cljs    "0.0-21"]] }
  }
  
  :clean-targets ^{:protect false} [
    "target"
    "release-js/datascript.bare.js"
    "release-js/datascript.js"
  ]
)
