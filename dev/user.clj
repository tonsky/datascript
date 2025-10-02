(ns user
  (:require
   [clj-reload.core :as reload]
   [clojure+.error]
   [clojure+.hashp]
   [clojure+.print]
   [clojure+.test]))

(clojure+.error/install!
  {:trace-transform
   (fn [trace]
     (take-while #(not (#{"Compiler" "clj-reload" "clojure-sublimed"} (:ns %))) trace))})

(clojure+.hashp/install!)
(clojure+.print/install!)
(clojure+.test/install!)

(reload/init
  {:dirs ["src" "bench" "test"]
   :no-reload '[user]})

(def reload
  reload/reload)

(defn test-all []
  (reload/reload {:only #"datascript\.test\.(?!cljs).*"})
  (clojure+.test/run))

(defn -test-main [_]
  (reload/reload {:only #"datascript\.test\.(?!cljs).*"})
  (let [{:keys [fail error]} (clojure+.test/run)]
    (System/exit (+ fail error))))
