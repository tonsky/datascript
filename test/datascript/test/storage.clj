(ns datascript.test.storage
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.test :as t :refer [is are deftest testing]]
    [cognitect.transit :as transit]
    [datascript.core :as d]
    [datascript.storage :as storage]
    [datascript.test.core :as tdc]))

(defrecord Storage [*disk *reads *writes *deletes]
  storage/IStorage
  (-store [_ addr+data-seq]
    (doseq [[addr data] addr+data-seq]
      (vswap! *disk assoc addr (pr-str data))
      (vswap! *writes conj addr)))
  
  (-restore [_ addr]
    (vswap! *reads conj addr)
    (-> @*disk (get addr) edn/read-string))

  (-list-addresses [_]
    (keys @*disk))

  (-delete [_ addrs-seq]
    (doseq [addr addrs-seq]
      (vswap! *disk dissoc addr)
      (vswap! *deletes conj addr))))

(defn make-storage [& [opts]]
  (map->Storage
    {:*disk    (volatile! {})
     :*reads   (volatile! [])
     :*writes  (volatile! [])
     :*deletes (volatile! [])}))  

(defn reset-stats [storage]
  (vreset! (:*reads storage) [])
  (vreset! (:*writes storage) [])
  (vreset! (:*deletes storage) []))

(defn small-db [& [opts]]
  (-> (d/empty-db nil (merge {:branching-factor 32, :ref-type :strong} opts))
    (d/db-with [[:db/add 1 :name "Ivan"]
                [:db/add 2 :name "Oleg"]
                [:db/add 3 :name "Petr"]])))

(defn large-db [& [opts]]
  (d/db-with 
    (d/empty-db nil (merge {:branching-factor 32, :ref-type :strong} opts))
    (map #(vector :db/add % :str (str %)) (range 1 1001))))

(deftest test-basics
  (testing "empty db"
    (let [db (d/empty-db)
          storage (make-storage)]
      (d/store! db storage)
      (is (= 5 (count @(:*writes storage))))
      (let [db' (d/restore storage)]
        (is (= 2 (count @(:*reads storage))))   ;; read root + tail
        (is (= db db'))                         ;; read eavt
        (is (= 3 (count @(:*reads storage)))))))
  
  (testing "small db"
    (let [db      (small-db)
          storage (make-storage)]
      (testing "store!"
        (d/store! db storage)
        (is (= 0 (count @(:*reads storage))))
        (is (= 5 (count @(:*writes storage)))))  ;; write root, tail + 1 level * 3 indexes
      (testing "restore"
        (let [db' (d/restore storage)]
          (is (= 2 (count @(:*reads storage)))) ;; read root + tail
          (is (= db db'))                       ;; read eavt
          (is (= 3 (count @(:*reads storage))))
          (vec (d/datoms db' :aevt))            ;; read aevt
          (is (= 4 (count @(:*reads storage))))
          (vec (d/datoms db' :avet))            ;; read avet
          (is (= 5 (count @(:*reads storage)))))
        
        (testing "count"
          (reset-stats storage)
          (let [db' (d/restore storage)]
            (count db')
            (is (= 3 (count @(:*reads storage))))))
    
        (testing "settings"
          (let [db' (d/restore storage)]
            (is (= {:branching-factor 32, :ref-type :strong} (d/settings db'))))))))
  
  (testing "large db"
    (let [db      (large-db)
          storage (make-storage)]
    
      (testing "store!"
        (d/store! db storage)
        (is (= 135 (count @(:*writes storage))))  ;; root, tail, avet root + 66 * 2 indexes
    
        (d/store! db)
        (is (= 135 (count @(:*writes storage))))) ;; store nothing if nothing changed
    
      (testing "restore"
        (let [db' (d/restore storage)]
          (is (= 2 (count @(:*reads storage)))) ;; read root + tail
      
          (is (= [1 :str "1"] (-> (d/datoms db' :eavt) first ((juxt :e :a :v)))))
          (is (= 5 (count @(:*reads storage)))) ;; eavt root + 2 levels
      
          (first (d/datoms db' :eavt))
          (is (= 5 (count @(:*reads storage)))) ;; second time no read
      
          (vec (d/datoms db' :eavt))
          (is (= 68 (count @(:*reads storage))))
      
          (vec (d/datoms db' :eavt))
          (is (= 68 (count @(:*reads storage)))) ;; second time no read
          
          (is (= db db'))
          (is (= (:eavt db) (:eavt db')))
          (is (= (:aevt db) (:aevt db')))
          (is (= (:avet db) (:avet db')))))
      
      (testing "count"
        (reset-stats storage)
        (let [db' (d/restore storage)]
          (= 1000 (count db'))
          (is (= 68 (count @(:*reads storage))))))
    
      (testing "incremental store"
        (reset-stats storage)
        (let [db' (d/db-with db [[:db/add 1001 :str "1001"]])]
          (d/store! db')
          (is (= 8 (count @(:*writes storage))))))) ;; root, tail + 3 leves * 2 indexes
    ))

(defmacro with-dir [dir & body]
  `(let [dir# ^java.io.File (io/file ~dir)]
     (try
       (.mkdirs dir#)
       ~@body
       (finally
         (doseq [file# (reverse (file-seq dir#))]
           (.delete ^java.io.File file#))))))

(def temp-dir
  "target/test_storage")

(defn streaming-edn-storage [dir]
  (d/file-storage dir))

(defn inmemory-edn-storage [dir]
  (d/file-storage dir
    {:freeze-fn pr-str
     :thaw-fn   edn/read-string}))

(defn streaming-transit-json-storage [dir]
  (d/file-storage dir
    {:read-fn  (fn [is]
                 (transit/read (transit/reader is :json)))
     :write-fn (fn [os o]
                 (transit/write (transit/writer os :json) o))}))

(defn inmemory-transit-json-storage [dir]
  (d/file-storage dir
    {:freeze-fn tdc/transit-write-str
     :thaw-fn tdc/transit-read-str}))

(defn streaming-transit-msgpack-storage [dir]
  (d/file-storage dir
    {:read-fn  (fn [is]
                 (transit/read (transit/reader is :msgpack)))
     :write-fn (fn [os o]
                 (transit/write (transit/writer os :msgpack) o))}))

(deftest test-file-storage
  (doseq [[format storage] [["streaming-edn"             (streaming-edn-storage temp-dir)]
                            ["inmemory-edn"              (inmemory-edn-storage temp-dir)]
                            ["streaming-transit-json"    (streaming-transit-json-storage temp-dir)]
                            ["inmemory-transit-json"     (inmemory-transit-json-storage temp-dir)]
                            ["streaming-transit-msgpack" (streaming-transit-msgpack-storage temp-dir)]]
          ref       [:strong :soft :weak]
          order     [32 64 512]
          :let      [opts {:branching-factor order, :ref-type ref}]
          [size db] [["empty" (d/empty-db nil opts)
                      "small" (small-db opts)
                      "large" (large-db opts)]]]
    (testing (str "storage: " format)
      (testing (str "ref-type: " ref)
        (testing (str "branching-factor: " order)
          (testing (str "size: " size)
            (with-dir temp-dir
              (d/store! db storage)
              (let [db'     (d/restore storage)]
                (is (= db db'))
                (is (= (:eavt db) (:eavt db')))
                (is (= (:aevt db) (:aevt db')))
                (is (= (:avet db) (:avet db')))))))))))

(deftest test-gc
  (let [storage (make-storage)
        db      (large-db {:storage storage})]
    (d/store! db)
    (is (= 135 (count (d/addresses db))))
    (is (= 135 (count (storage/-list-addresses storage))))
    (is (= (d/addresses db) (set (storage/-list-addresses storage))))
    
    (let [db' (d/db-with db [[:db/add 1001 :str "1001"]])]
      (d/store! db')
      (is (> (count (storage/-list-addresses storage))
            (count (d/addresses db'))))
      
      (d/collect-garbage! db')
      (is (= (d/addresses db') (set (storage/-list-addresses storage))))
      
      (is (= 6 (count @(:*deletes storage)))))))

(deftest test-conn
  (let [storage (make-storage)
        conn    (d/create-conn nil {:storage          storage
                                    :branching-factor 32
                                    :ref-type         :strong})]
    (is (= 5 (count @(:*writes storage)))) ;; initial store
    
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (is (= 6 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    
    ;; only writing tail
    (d/transact! conn [[:db/add 2 :name "Oleg"]])
    (is (= 7 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    (is (= 2 (count @(:tx-tail (meta conn)))))
    (is (= 2 (count (apply concat @(:tx-tail (meta conn))))))
    
    ;; bigger tx, still writing tail
    (d/transact! conn (mapv #(vector :db/add % :name (str %)) (range 3 33)))
    (is (= 8 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    (is (= 3 (count @(:tx-tail (meta conn)))))
    (is (= 32 (count (apply concat @(:tx-tail (meta conn))))))
    
    ;; tail overflows, flush db
    (d/transact! conn [[:db/add 33 :name "Petr"]])
    (is (= 16 (count @(:*writes storage))))
    
    ;; and start over
    (d/transact! conn [[:db/add 34 :name "Anna"]])
    (is (= 17 (count @(:*writes storage))))
    (is (= @#'storage/tail-addr (last @(:*writes storage))))
    
    ;; restore conn with tail
    (let [conn' (d/restore-conn storage)]
      (is (= @conn @conn'))
      
      ;; transact keeps working on restored conn
      (d/transact! conn' [[:db/add 35 :name "Vera"]])
      (is (= 18 (count @(:*writes storage))))
      (is (= @#'storage/tail-addr (last @(:*writes storage))))
      
      ;; overflow keeps working on restored conn
      (d/transact! conn' (mapv #(vector :db/add % :name (str %)) (range 36 80)))
      (is (= 28 (count @(:*writes storage))))
      (is (= @#'storage/tail-addr (last @(:*writes storage))))
      
      ;; restore conn without tail
      (let [conn'' (d/restore-conn storage)]
        (is (= @conn' @conn''))
      
        (d/transact! conn'' [[:db/add 80 :name "Ilya"]])
        (is (= 29 (count @(:*writes storage))))
        (is (= @#'storage/tail-addr (last @(:*writes storage))))
        
        ;; gc on conn
        (is (> (count (storage/-list-addresses storage))
              (count (d/addresses @(:db-last-stored (meta conn''))))))
        
        (d/collect-garbage-conn! conn'')
        (is (= (count (storage/-list-addresses storage))
              (count (d/addresses @(:db-last-stored (meta conn''))))))
        
        (let [conn''' (d/restore-conn storage)]
          (is (= @conn'' @conn''')))))))


; (t/test-ns *ns*)
; (t/run-test-var #'test-conn)

(comment  
  (let [serializable (with-open [is (io/input-stream (io/file "/Users/tonsky/ws/roam/db_3M.json_transit"))]
                       (transit/read (transit/reader is :json)))]
    (def db (d/from-serializable serializable {:branching-factor 512}))
    (count db))
       
  (d/store! db (streaming-edn-storage "target/db_streaming_edn"))                         ;; ~10 sec
  (d/store! db (inmemory-edn-storage "target/db_inmemory_edn"))                           ;; ~10 sec
  (d/store! db (streaming-transit-json-storage "target/db_streaming_transit_json"))       ;; ~7.5 sec
  (d/store! db (inmemory-transit-json-storage "target/db_inmemory_transit_json"))         ;; ~6.4 sec
  (d/store! db (streaming-transit-msgpack-storage "target/db_streaming_transit_msgpack")) ;; ~6.3 sec
  
  (def storage
    (streaming-edn-storage "target/db_streaming_edn"))
  
  (def db' (d/restore storage))
  
  (count (d/addresses db'))
  (count (storage/-list-addresses storage))
  (d/collect-garbage! db')

  (first (:eavt db'))
  
  (->> (:eavt db')
    (drop 5000)
    (take 5000)))  
