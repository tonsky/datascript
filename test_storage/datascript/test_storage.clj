(ns datascript.test-storage
  (:refer-clojure :exclude [time])
  (:require    
    [clojure.java.io :as io]
    [clojure.string :as str]
    [cheshire.core :as json]
    [cognitect.transit :as t]
    [datascript.db :as db]
    [datascript.core :as d]
    [me.tonsky.persistent-sorted-set :as set])
  (:import
    [datascript.db Datom]
    [java.io ByteArrayInputStream ByteArrayOutputStream]
    [datascript.core IStorage]))

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

(def storage ;; 12 sec
  (d/file-storage "target/db_edn"))

(def storage ;; 99 sec
  (d/file-storage "target/db_json"
    {:read-fn  (fn [is]
                 (t/read (t/reader is :json)))
     :write-fn (fn [os o]
                 (t/write (t/writer os :json) o))}))

(def storage ;; 5.9 sec
  (d/file-storage "target/db_json"
    {:write-fn (fn [os o]
                 (with-open [wrt (io/writer os)]
                   (.write ^java.io.Writer wrt ^String (write-transit-str o))))
     :read-fn  (fn [is]
                 (read-transit-str (slurp (io/reader is))))}))

(def storage ;; 5.7 sec
  (d/file-storage "target/db_msgpack"
    {:read-fn  (fn [is]
                 (t/read (t/reader is :msgpack)))
     :write-fn (fn [os o]
                 (t/write (t/writer os :msgpack) o))}))

(comment
  (set/set-branching-factor! 512)
  me.tonsky.persistent_sorted_set.PersistentSortedSet/MAX_LEN

  (let [json (with-open [rdr (io/reader (io/file "/Users/tonsky/ws/roam/db_3M.json"))]
               (json/parse-stream rdr))]
    (def db (d/from-serializable json))
    (count db))

  (d/store storage db)
    
  (def db' (d/restore storage))

  (first (:eavt db'))
  
  (->> (:eavt db')
    (drop 5000)
    (take 5000)))  
