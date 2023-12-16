(ns user
  (:require
    [clojure.core.server :as server]
    [clojure.java.io :as io]
    [clojure.test :as t]
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

(defn -main [& args]
  ;; setup repl
  (let [args (apply array-map args)
        port (or
               (some-> (get args "--port") parse-long)
               (+ 1024 (rand-int 64512)))
        file (io/file ".repl-port")]
    (println "Started Server Socket REPL on port" port)
    (spit file port)
    (.deleteOnExit file)
    (server/start-server
      {:name          "repl"
       :port          port
       :accept        'clojure.core.server/repl
       :server-daemon false})))

(defn test-all []
  (reload)
  (t/run-all-tests #"datascript\..*"))