(defproject datascript "0.11.0"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure "1.7.0-beta2" :scope "provided"]
    [org.clojure/clojurescript "0.0-3211" :scope "provided"]
  ]
  
  :global-vars {
    *warn-on-reflection* true
    *unchecked-math* :warn-on-boxed
  }
  
  :jvm-opts ["-Xmx2g"]

  ;; good to have a single acceptance command in the meanwhile; maybe throwaway after move-to-cljc?
  :aliases {
    "test-clj"        ["test" "datascript.test.core" "datascript.test.btset" "datascript.test.entity"]
    "test-cljs"       ["cljsbuild" "test"]
    "clean-test-all"  ["do" "clean," "test-cljs," "test-clj"]
  }
  
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
      :source-paths ["dev"] ; for user.clj
      :plugins [
        [lein-cljsbuild "1.0.5"]
        [com.cemerick/clojurescript.test "0.3.3"]
        [com.cemerick/austin "0.1.6"]
      ]
      :cljsbuild { 
        :builds [
          { :id "advanced"
            :source-paths ["src" "test"]
            :compiler {
              :main          datascript.test
              :output-to     "target/datascript.js"
;;               :output-dir    "target/advanced"
              :optimizations :whitespace #_ :advanced
              :source-map    "target/datascript.js.map"
              :pretty-print  true
              :warnings     {:single-segment-namespace false}
              :recompile-dependents false
            }}
          { :id "whitespace"
            :source-paths ["src" "test"]
            :compiler {
              :main          datascript.test
              :optimizations :whitespace
              :pretty-print  true
              :output-dir    "target/whitespace"
              :output-to     "target/whitespace/datascript.js"
              :source-map    "target/whitespace/datascript.js.map"
              :warnings      {:single-segment-namespace false}
              :recompile-dependents false
            }}
        ]
        :test-commands {
          "datascript.test"    [ "phantomjs" :runner "target/whitespace/datascript.js" ]
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
