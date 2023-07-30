(ns datascript.test-storage
  (:refer-clojure :exclude [time])
  (:require    
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [cheshire.core :as json]
    [cognitect.transit :as t]
    [datascript.db :as db]
    [datascript.core :as d]
    [datascript.storage :as storage]
    [me.tonsky.persistent-sorted-set :as set])
  (:import
    [datascript.db Datom]
    [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn read-transit [is]
  (t/read (t/reader is :json)))

(defn read-transit-str [^String s]
  (read-transit (ByteArrayInputStream. (.getBytes s "UTF-8"))))

(defn write-transit [o os]
  (t/write (t/writer os :json) o))

(defn write-transit-bytes ^bytes [o]
  (let [os (ByteArrayOutputStream.)]
    (write-transit o os)
    (.toByteArray os)))
    
(defn write-transit-str [o]
  (String. (write-transit-bytes o) "UTF-8"))

(def *stats
  (atom
    {:reads 0
     :writes 0}))

(def *cache
  (atom {}))

(defmacro with-stats [& body]
  `(let [t0#  (System/currentTimeMillis)
         _#   (reset! *stats {:reads 0, :writes 0})
         res# (do ~@body)
         res# (if (map? res#) res# {:res res#})
         dt#  (- (System/currentTimeMillis) t0#)]
     (merge {:time dt#} @*stats res#)))

(def streaming-edn-storage
  (d/file-storage "target/db_streaming_edn"))

(def inmemory-edn-storage
  (d/file-storage "target/db_inmemory_edn"
    {:freeze-fn pr-str
     :thaw-fn   edn/read-string}))

(def streaming-transit-json-storage
  (d/file-storage "target/db_streaming_transint_json"
    {:read-fn  (fn [is]
                 (t/read (t/reader is :json)))
     :write-fn (fn [os o]
                 (t/write (t/writer os :json) o))}))

(def inmemory-transit-json-storage
  (d/file-storage "target/db_inmemory_transit_json"
    {:freeze-fn write-transit-str
     :thaw-fn read-transit-str}))

(def streaming-transit-msgpack-storage
  (d/file-storage "target/db_streaming_transit_msgpack"
    {:read-fn  (fn [is]
                 (t/read (t/reader is :msgpack)))
     :write-fn (fn [os o]
                 (t/write (t/writer os :msgpack) o))}))

(comment
  (let [json (with-open [rdr (io/reader (io/file "/Users/tonsky/ws/roam/db_3M.json"))]
               (json/parse-stream rdr))]
    (def db (d/from-serializable json {:branching-factor 512}))
    (count db))
  
  (d/empty-db {} {:storage streaming-edn-storage})
  
  (d/store! (d/empty-db) streaming-edn-storage)
     
  (d/store! db streaming-edn-storage)             ;; 10 sec
  (d/store! db inmemory-edn-storage)              ;; 10 sec
  (d/store! db streaming-transit-json-storage)    ;; 7.5 sec
  (d/store! db inmemory-transit-json-storage)     ;; 6.4 sec
  (d/store! db streaming-transit-msgpack-storage) ;; 6.3 sec
  
  (def db' (d/restore streaming-edn-storage))
  
  (count (d/addresses db'))
  (count (storage/-list-addresses streaming-edn-storage))
  (d/collect-garbage! db')

  (first (:eavt db'))
  
  (->> (:eavt db')
    (drop 5000)
    (take 5000)))  
