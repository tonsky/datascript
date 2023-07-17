(ns roam
  (:require
    [fs]
    [cognitect.transit :as transit]
    [datascript.bench.bench :refer-macros [dotime measure]]
    [datascript.core :as d]
    [datascript.db :as db]))

(def read-handlers
  {"datascript/DB"    (transit/read-handler db/db-from-reader)
   "datascript/Datom" (transit/read-handler db/datom-from-reader)})

(defn transit-read [s type]
  (transit/read (transit/reader type {:handlers read-handlers}) s))

(defn transit-read-str [s]
  (transit-read s :json))

(def file
  (fs/readFileSync "/Users/tonsky/ws/roam/db_3M.json"))
  
(def parsed
  (js/JSON.parse file))
  
(def db
  (d/from-serializable parsed))

(def ids
  (let [index (vec (d/datoms db :aevt :block/parents))]
    (->> #(rand-nth index)
      (repeatedly 10000)
      (map :e)
      (into #{}))))

(defn map-to [ks val-fn]
  (persistent!
    (reduce
      (fn [m k]
        (assoc! m k (val-fn k)))
      (transient {})
      ks)))

(comment
  (count db)    
  (count ids)
  (count (d/datoms db :aevt :block/parents))

  (dotime 10000
    (let [index (js/Object.)]
      (doseq [d (d/datoms db :aevt :block/parents)]
        (aset index (.-e d) (.-v d)))
      (map-to ids #(aget index %))))

  (dotime 10000
    (map-to ids
      #(-> (d/datoms db :eavt % :block/parents)
         (first)
         (.-v))))
  
  (dotime 10000
    (map-to ids
      #(-> (d/datoms db :aevt :block/parents %)
         (first)
         (.-v))))
  )

(defn -main [& args]
  (println "Loaded" (count db) "datoms"))
