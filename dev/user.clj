(ns user
  (:require
    [clojure.test :as t]
    [clojure.string :as str]
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

(defn test-ns [ns]
  (reset-env
    (clojure.test/test-ns ns)))

(defn retest-all []
  (clojure.tools.namespace.repl/refresh)
  (reset-env
    (clojure.test/run-all-tests #"datascript\.test\.(?!btset).*")))

#_(retest-all)

;; Fix clojure.test exception printing to only print user stack elements & demunge names

(defn file-name [e]
  (when-some [file (.getFileName e)]
    (->> (str/split file #"/") (last))))

(defn print-trace-element
  "Prints a Clojure-oriented view of one element in a stack trace."
  {:added "1.1"}
  [prefix e]
  (let [class (.getClassName e)]
    (when (and (not (str/starts-with? class "clojure."))
               (not (str/starts-with? class "nrepl."))
               (not (str/starts-with? class "java.")))
      (print prefix)
      (let [method     (.getMethodName e)
            [_ ns fun] (re-matches #"([A-Za-z0-9_.-]+)\$([A-Za-z0-9_.-]+)" (str class))]
        (if (and ns fun (#{"invoke" "invokeStatic" "doInvoke"} method))
	        (printf "%s/%s" (clojure.lang.Compiler/demunge ns) (clojure.lang.Compiler/demunge fun))
	        (printf "%s.%s" class method)))
      (printf " (%s:%d)\n" (or (file-name e) "") (.getLineNumber e)))))

(defn print-throwable
  "Prints the class and message of a Throwable."
  {:added "1.1"}
  [tr]
  (printf "%s: %s" (.getName (class tr)) (.getMessage tr)))

(defn print-stack-trace
  "Prints a Clojure-oriented stack trace of tr, a Throwable.
  Prints a maximum of n stack frames (default: unlimited).
  Does not print chained exceptions (causes)."
  {:added "1.1"}
  ([tr] (print-stack-trace tr nil))
  ([^Throwable tr n]
     (let [st (.getStackTrace tr)]
       (print-throwable tr)
       (newline)
       (print " at\n") 
       (if-let [e (first st)]
         (print-trace-element "" e)
         (println "[empty stack trace]"))
       (doseq [e (if (nil? n)
		               (rest st)
		               (take (dec n) (rest st)))]
	       (print-trace-element "    " e)))))

(defn print-cause-trace
  "Like print-stack-trace but prints chained exceptions (causes)."
  {:added "1.1"}
  ([tr] (print-cause-trace tr nil))
  ([tr n]
     (print-stack-trace tr n)
     (when-let [cause (.getCause tr)]
       (print "Caused by: " )
       (recur cause n))))

(defmethod t/report :error [m]
  (t/with-test-out
   (t/inc-report-counter :error)
   (println "\nERROR in" (t/testing-vars-str m))
   (when (seq t/*testing-contexts*) (println (t/testing-contexts-str)))
   (when-let [message (:message m)] (println message))
   (println "expected:" (pr-str (:expected m)))
   (print "  actual: ")
   (let [actual (:actual m)]
     (if (instance? Throwable actual)
       (print-cause-trace actual t/*stack-trace-depth*)
       (prn actual)))))

(defmethod t/report :begin-test-ns [m]
  (t/with-test-out
    (println "Testing" (ns-name (:ns m)))))
