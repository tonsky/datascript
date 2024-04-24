(ns user
  (:require
    [duti.core :as duti]))

(duti/set-dirs "src" "bench" "test" #_"bench_datomic" #_"test_datomic")

(def reload
  duti/reload)

(defn -main [& {:as args}]
  (set! *warn-on-reflection* true)
  (require 'datascript.test)
  (duti/start-socket-repl))

(defn test-all []
  (reload)
  (duti/test #"datascript\.test\..*"))

(defn -test-main [_]
  (reload {:only #"datascript\.test\..*"})
  (duti/test-exit #"datascript\.test\..*"))
