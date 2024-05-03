(def version "1.6.5")

(defproject datascript (str version (System/getenv "DATASCRIPT_CLASSIFIER"))
  :description "An implementation of Datomic in-memory database and Datalog query engine in ClojureScript"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datascript"
  
  :dependencies [
    [org.clojure/clojure         "1.10.2"   :scope "provided"]
    [org.clojure/clojurescript   "1.10.844" :scope "provided"]
    [persistent-sorted-set       "0.3.0"]
    [io.github.tonsky/extend-clj "0.1.0"]
  ]
  
  :plugins [
    [lein-cljsbuild "1.1.7"]
  ]
  
  :global-vars {
    *warn-on-reflection*   true
    *print-namespace-maps* false
;;     *unchecked-math* :warn-on-boxed
  }
  :jvm-opts ["-Xmx2g" "-server" "-Dclojure.main.report=stderr"]
  
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
        :source-paths ["src" "bench"]
        :compiler {
          :main          datascript.bench.datascript
          :output-to     "target/datascript.js"
          :optimizations :advanced
          ; :source-map    "target/datascript.js.map"
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
    :test    {:dependencies [[metosin/jsonista            "0.3.3"]
                             [cheshire                    "5.10.0"]
                             [com.cognitect/transit-clj   "1.0.324"]
                             [com.cognitect/transit-cljs "0.8.269"]]}
    :bench   {:dependencies [[criterium "0.4.6"]
                             [metosin/jsonista "0.3.3"]
                             [com.clojure-goes-fast/clj-async-profiler "0.5.1"]]}
    :datomic {:dependencies [[com.datomic/datomic-pro "1.0.6344"]]}
  }
  
  :clean-targets ^{:protect false} [
    "target"
    "release-js/datascript.bare.js"
    "release-js/datascript.js"
  ]
  
  :deploy-repositories
  {"clojars"
   {:url "https://clojars.org/repo"
    :username "tonsky"
    :password :env/clojars_token_datascript
    :sign-releases false}}
)
