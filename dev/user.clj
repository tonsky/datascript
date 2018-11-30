(ns user
  (:require
    clojure.test
    clojure.tools.namespace.repl
    cljs.repl
    cljs.build.api
    cljs.repl.node
    cljs.repl.browser))

(defrecord Dirs [dirs]
  cljs.closure/Inputs
  (-paths [_]
    (mapv clojure.java.io/file dirs))
  
  cljs.closure/Compilable
  (-compile [_ opts]
    (let [out-dir (cljs.util/output-directory opts)]
      (vec
        (for [src-dir dirs
              root    (cljs.compiler/compile-root src-dir out-dir opts)]
          (cljs.closure/compiled-file root))))))

(defn repl [main env & dirs]
  (cljs.build.api/build (Dirs. (concat ["src" "test"] dirs))
    {:main       main
     :output-to  "target/datascript.js"
     :output-dir "target/none"
     :warnings   {:single-segment-namespace false}
     :verbose    true})

  (cljs.repl/repl env
    :watch      (Dirs. (concat ["src" "test"] dirs))
    :output-dir "target/none"))

(defn browser-repl []
  (repl 'browser-repl (cljs.repl.browser/repl-env) "dev"))

(defn node-repl []
  (repl 'datascript (cljs.repl.node/repl-env)))

;; tests

(defmacro reset-env [& body]
  `(binding [*print-namespace-maps* false
             *data-readers* (merge *data-readers* @(resolve 'datascript.core/data-readers))]
     ~@body))

(defn test-var [var]
  (binding [clojure.test/*report-counters* (ref clojure.test/*initial-report-counters*)]
    (reset-env
      (clojure.test/test-vars [var]))
    @clojure.test/*report-counters*))

(defn retest-all []
  (clojure.tools.namespace.repl/refresh)
  (reset-env
    (clojure.test/run-all-tests #"datascript\.test\.(?!btset).*")))

#_(retest-all)
