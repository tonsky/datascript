(defproject datascript-bench "0.1.0"
  :dependencies [
    [org.clojure/clojure "1.7.0-RC1"]
    [org.clojure/clojurescript "0.0-3308"]
    [datascript ~(System/getenv "BENCH_VERSION")]
  ]
  
  :jvm-opts ["-Xmx2g" "-server"]
  
  :plugins [[lein-cljsbuild "1.0.6"]]
  
  :cljsbuild { 
    :builds [
      { :id "advanced"
        :source-paths ["src"]
        :assert false
        :compiler {
          :output-to     "target/datascript.js"
          :optimizations :advanced
          :pretty-print  false
          :elide-asserts true
          :warnings      {:single-segment-namespace false}
          :recompile-dependents false
        }
  }]}
)
