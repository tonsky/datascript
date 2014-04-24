(defproject datascript "0.1.3"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure "1.6.0"]
    [org.clojure/clojurescript "0.0-2173"] ;; 0.0-2202
  ]
  :profiles {
    :dev {
      :plugins [
        [lein-cljsbuild "1.0.2"] ;; 1.0.3
        [com.cemerick/clojurescript.test "0.3.0"]
      ]
      :cljsbuild { 
        :builds [
          { :id "dev"
            :source-paths ["src" "test"]
            :compiler {
              :output-to     "web/datascript.js"
              :output-dir    "web/out"
              :optimizations :none
              :source-map    true
            }}
          { :id "testable"
            :source-paths ["src" "test"]
            :compiler {
              :output-to     "web/datascript.testable.js"
              :optimizations :whitespace
            }}
        ]
        :test-commands {
          "unit-tests" [
            "phantomjs" :runner
            "web/datascript.testable.js"
          ]}
      }
    }
  })
