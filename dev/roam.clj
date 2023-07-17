(ns roam
  (:require
    [clojure.java.io :as io]
    [cognitect.transit :as transit]
    [datascript.core :as d]
    [datascript.db :as db]))

(def read-handlers
  {"datascript/DB"    (transit/read-handler db/db-from-reader)
   "datascript/Datom" (transit/read-handler db/datom-from-reader)})

(defn transit-read [s type]
  (with-open [is (java.io.ByteArrayInputStream. s)]
    (transit/read (transit/reader is type {:handlers read-handlers}))))

(defn transit-read-str [s]
  (transit-read (.getBytes ^String s "UTF-8") :json))

(defonce db
  (with-open [is (io/input-stream "../roam/db_3M.transit")]
    (transit/read (transit/reader is :json {:handlers read-handlers}))))

(def ids
  (let [index (vec (d/datoms db :aevt :block/parents))]
    (->> #(rand-nth index)
      (repeatedly 10000)
      (map :e)
      (into #{}))))  

(comment
  (time
    (persistent!
      (reduce
        (fn [m eid]
          (let [parents (d/datoms db :aevt :block/parents eid)]
            (assoc! m eid (:v (first parents)))))
        (transient {})
        ids)))

  (count (d/datoms db :aevt :block/parents))

  (time
    (let [index (persistent!
                  (reduce
                    (fn [m d]
                      (assoc! m (:e d) (:v d)))
                    (transient {})
                    (d/datoms db :aevt :block/parents)))]
      (persistent!
        (reduce
          (fn [m eid]
            (assoc! m eid (get index eid)))
          (transient {})
          ids))))
  )

