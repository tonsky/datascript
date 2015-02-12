(defproject datascript "0.9.0"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure "1.6.0" :scope "provided"]
    [org.clojure/clojurescript "0.0-2816" :scope "provided"]
  ]
  
  :global-vars {
    *warn-on-reflection* true
    *unchecked-math* :warn-on-boxed
  }
  
  :jvm-opts ["-Xmx2g"]
  
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
        }
        :notify-command ["release-js/wrap_bare.sh"]}
  ]}

  :profiles {
    :dev {
      :plugins [
        [lein-cljsbuild "1.0.3"]
        [com.cemerick/clojurescript.test "0.3.1"]
      ]
      :cljsbuild { 
        :builds [
          { :id "dev"
            :source-paths ["src" "test"]
            :compiler {
              :output-to     "web/datascript.js"
              :output-dir    "web/target-cljs"
              :optimizations :none
              :source-map    true
              :warnings     {:single-segment-namespace false}
              :recompile-dependents false
            }}
          { :id "testable"
            :source-paths ["src" "test"]
            :compiler {
              :output-to     "web/datascript.testable.js"
              :optimizations :advanced
              :pretty-print  true
              :warnings     {:single-segment-namespace false}
            }}
        ]
        :test-commands {
          "datascript.test"    [ "phantomjs" :runner "web/datascript.testable.js" ]
          "datascript.test.js" [ "phantomjs" "test/js/runner.js" ]
        }
      }
    }
  }
  
  :clean-targets ^{:protect false} [
    "target"
    "web/target-cljs"
    "web/datascript.js"
    "web/datascript.testable.js"
    "release-js/datascript.bare.js"
    "release-js/datascript.js"
  ]
)
