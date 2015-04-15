(defproject datascript "0.9.0"
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure "1.6.0" :scope "provided"]
    [org.clojure/clojurescript "0.0-2727" :scope "provided"]
    [org.clojure/core.incubator "0.1.3"]
    [danlentz/clj-uuid "0.1.5"]
    ;; XXX only for dev, not in production; elide?
    ;; [com.taoensso/timbre "3.4.0"]
    ;; [prismatic/schema "0.4.0"]
  ]
  
  :global-vars {
    *warn-on-reflection* true
    *unchecked-math* :warn-on-boxed
  }
  
  :jvm-opts ["-Xmx2g"]

  :auto-clean false
  
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

  :source-paths ["src/clj"
                 "target/generated/src/clj"]

  :test-paths ["test/clj"
               "target/generated/test/clj"]

  :profiles {
    :dev {
      :source-paths ["dev"] ; for user.clj
      :plugins [
        [com.keminglabs/cljx "0.6.0"]
        [lein-cljsbuild "1.0.3"]
        [com.cemerick/clojurescript.test "0.3.1"]
        [com.cemerick/austin "0.1.6"]
      ]
      :cljsbuild { 
        :builds [
          { :id "dev"
            :source-paths ["src/cljs"  "target/generated/src/cljs"
                           "test/cljs" "target/generated/test/cljs"]
            :compiler {
              :output-to     "web/datascript.js"
              :output-dir    "web/target-cljs"
              :optimizations :none
              :source-map    true
            }}
          { :id "testable"
            :source-paths ["src/cljs"  "target/generated/src/cljs"
                           "test/cljs" "target/generated/test/cljs"]
            :compiler {
              :output-dir    "web"
              :output-to     "web/datascript.testable.js"
              :source-map    "web/datascript.testable.js.map"
              :optimizations :whitespace
              :pretty-print  true
            }}
        ]
        :test-commands {
          "datascript.test"    [ "phantomjs" :runner "web/datascript.testable.js" ]
          "datascript.test.js" [ "phantomjs" "test/cljs/js/runner.js" ]
        }
      }
    }
  }
  
  :cljx {:builds [{:source-paths ["src/cljx"]
                   :output-path "target/generated/src/clj"
                   :rules :clj}
                  {:source-paths ["test/cljx"]
                   :output-path "target/generated/test/clj"
                   :rules :clj}

                  {:source-paths ["src/cljx"]
                   :output-path "target/generated/src/cljs"
                   :rules :cljs}
                  {:source-paths ["test/cljx"]
                   :output-path "target/generated/test/cljs"
                   :rules :cljs}
                  ]}

  :clean-targets ^{:protect false} [
    "target"
    "web/target-cljs"
    "web/datascript.js"
    "web/datascript.testable.js"
    "release-js/datascript.bare.js"
    "release-js/datascript.js"
  ]
)
