(defproject tonsky/datomicscript "0.1.0-SNAPSHOT"
  :dependencies [
    [org.clojure/clojure "1.6.0"]
    [org.clojure/clojurescript "0.0-2156"] ;; 0.0-2202
  ]
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
      { :id "prod"
        :source-paths ["src"]
        :compiler {
          :pretty-print   false
          :output-to     "web/datomicscript.min.js"
          :optimizations :advanced
        }}
  ]}
)
