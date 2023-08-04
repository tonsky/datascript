(ns user
  (:require
    [clojure.tools.namespace.repl :as ns]))

(ns/set-refresh-dirs "src" "bench" "test" #_"bench_datomic" #_"test_datomic")

(defn reload []
  (set! *warn-on-reflection* true)
  (let [res (ns/refresh)]
    (if (instance? Throwable res)
      (do
        (.printStackTrace ^Throwable res)
        (throw res))
      res)))

(def lock
  (Object.))

(defn position []
  (let [trace (->> (Thread/currentThread)
                (.getStackTrace)
                (seq))
        el    ^StackTraceElement (nth trace 4)]
    (str "[" (clojure.lang.Compiler/demunge (.getClassName el)) " " (.getFileName el) ":" (.getLineNumber el) "]")))

(defn p [form]
  `(let [t# (System/currentTimeMillis)
         res# ~form]
     (locking lock
       (println (str "#p" (position) " " '~form " => (" (- (System/currentTimeMillis) t#) " ms) " res#)))
     res#))
