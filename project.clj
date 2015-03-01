(defproject datascript "0.9.0"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure "1.6.0" :scope "provided"]
    [org.clojure/clojurescript "0.0-2913" :scope "provided"]
  ]
  
  :hooks [leiningen.cljsbuild]
  
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
          :warnings      {:single-segment-namespace false}
        }
        :notify-command ["release-js/wrap_bare.sh"]}
  ]}

  :profiles {
    :dev {
      :plugins [
        [lein-cljsbuild "1.0.5"]
        [com.cemerick/clojurescript.test "0.3.2"]
      ]
      :cljsbuild { 
        :builds [
          { :id "advanced"
            :source-paths ["src" "test"]
            :compiler {
              :main          datascript.test
              :output-to     "target/datascript.js"
;;               :output-dir    "target/advanced"
              :optimizations :advanced
              :source-map    "target/datascript.js.map"
              :pretty-print  true
              :warnings     {:single-segment-namespace false}
              :recompile-dependents false
            }}
          { :id "none"
            :source-paths ["src" "test"]
            :compiler {
              :main          datascript.test
              :output-to     "target/datascript.js"
              :output-dir    "target/none"
              :optimizations :none
              :source-map    true
              :warnings     {:single-segment-namespace false}
              :recompile-dependents false
            }}
        ]
        :test-commands {
          "datascript.test"    [ "phantomjs" :runner "target/datascript.js" ]
          "datascript.test.js" [ "phantomjs" "test/js/runner.js" ]
        }
      }
    }
  }
  
  :clean-targets ^{:protect false} [
    "target"
    "release-js/datascript.bare.js"
    "release-js/datascript.js"
  ]
)
