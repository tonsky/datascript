(def version "0.2.1")

(defproject datascript version
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
        :compiler {
          :output-to     ~(str "web/datascript-" version ".min.js")
          :optimizations :advanced
          :pretty-print  false
          :externs       ["src/datascript/externs.js"]
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
