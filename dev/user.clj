(ns user
  (:require
    [duti.core :as duti]))

(duti/set-dirs "src" "bench" "test" #_"bench_datomic" #_"test_datomic")

(def reload
  duti/reload)

(def -main
  duti/-main)

(defn test-all []
  (duti/test #"datascript\.test\.(?!cljs).*"))

(defn -test-main [_]
  (duti/test-exit #"datascript\.test\.(?!cljs).*"))
