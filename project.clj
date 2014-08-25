(defproject datascript "0.3.1"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure "1.6.0"]
    [org.clojure/clojurescript "0.0-2311"]
  ]
  :cljsbuild { 
    :builds [
      { :id "release"
        :source-paths ["src"]
        :assert false
        :compiler {
          :output-to     "web/datascript.min.js"
          :optimizations :advanced
          :pretty-print  false
          :externs       ["src/datascript/externs.js"]
          :preamble      ["datascript/preamble.js"]
          :elide-asserts true
        }}
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
          "test.datascript"    [ "phantomjs" :runner "web/datascript.testable.js" ]
          "test.datascript.js" [ "phantomjs" "test/js/js.js" ]
        }
      }
    }
  })
