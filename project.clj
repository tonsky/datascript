(defproject datomicscript "0.1.2"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datomicscript"
  
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
              :output-to     "web/datomicscript.js"
              :output-dir    "web/out"
              :optimizations :none
              :source-map    true
            }}
          { :id "testable"
            :source-paths ["src" "test"]
            :compiler {
              :output-to     "web/datomicscript.testable.js"
              :optimizations :whitespace
            }}
        ]
        :test-commands {
          "unit-tests" [
            "phantomjs" :runner
            "web/datomicscript.testable.js"
          ]}
      }
    }
  }
  :cljsbuild { 
    :builds [
      { :id "prod"
        :source-paths ["src"]
        :compiler {
          :pretty-print  false
          :output-to     "web/datomicscript.min.js"
          :optimizations :advanced
        }}]
  }
)
