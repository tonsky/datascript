(ns datascript.core
  (:refer-clojure :exclude [filter])
  (:require
    [datascript.db :as db #?@(:cljs [:refer [FilteredDB]])]
    [datascript.pull-api :as dp]
    [datascript.query :as dq]
    [datascript.impl.entity :as de]
    [datascript.btset :as btset])
  #?(:clj
    (:import
      [datascript.db FilteredDB]
      [datascript.impl.entity Entity]
      [java.util UUID])))

;; SUMMING UP

(def ^:export  q dq/q)
(def ^:export  entity de/entity)
(defn ^:export entity-db [^Entity entity]
  {:pre [(de/entity? entity)]}
  (.-db entity))

(def ^:export datom db/datom)

(def ^:export pull dp/pull)
(def ^:export pull-many dp/pull-many)
(def ^:export touch de/touch)

(def ^:export empty-db db/empty-db)
(def ^:export init-db db/init-db)

(def ^:export datom? db/datom?)
(def ^:export db? db/db?)

(def ^:export ^:const tx0 db/tx0)

(defn ^:export is-filtered [x]
  (instance? FilteredDB x))

(defn ^:export filter [db pred]
  {:pre [(db/db? db)]}
  (if (is-filtered db)
    (let [^FilteredDB fdb db
          u (.-unfiltered-db fdb)]
      (FilteredDB. u #(and (pred u %) ((.-pred fdb) %)) #?(:clj (atom nil))))
    (FilteredDB. db #(pred db %) #?(:clj (atom nil)))))

(defn ^:export with
  ([db tx-data] (with db tx-data nil))
  ([db tx-data tx-meta]
    {:pre [(db/db? db)]}
    (if (is-filtered db)
      (throw (ex-info "Filtered DB cannot be modified" {:error :transaction/filtered}))
      (db/transact-tx-data (db/map->TxReport
                             { :db-before db
                               :db-after  db
                               :tx-data   []
                               :tempids   {}
                               :tx-meta   tx-meta}) tx-data))))

(defn ^:export db-with [db tx-data]
  {:pre [(db/db? db)]}
  (:db-after (with db tx-data)))

(defn ^:export datoms
  ([db index]             {:pre [(db/db? db)]} (db/-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3 c4])))

(defn ^:export seek-datoms
  ([db index]             {:pre [(db/db? db)]} (db/-seek-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-seek-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3 c4])))

(defn ^:export index-range [db attr start end]
  {:pre [(db/db? db)]}
  (db/-index-range db attr start end))

(def ^:export entid db/entid)

;; Conn

(defn ^:export conn? [conn]
  (and #?(:clj  (instance? clojure.lang.IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
    (db/db? @conn)))

(defn ^:export conn-from-db [db]
  (atom db :meta { :listeners (atom {}) }))

(defn ^:export conn-from-datoms
  ([datoms]        (conn-from-db (init-db datoms)))
  ([datoms schema] (conn-from-db (init-db datoms schema))))

(defn ^:export create-conn
  ([]       (conn-from-db (empty-db)))
  ([schema] (conn-from-db (empty-db schema))))

(defn -transact! [conn tx-data tx-meta]
  {:pre [(conn? conn)]}
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with db tx-data tx-meta)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn ^:export transact!
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
    {:pre [(conn? conn)]}
    (let [report (-transact! conn tx-data tx-meta)]
      (doseq [[_ callback] @(:listeners (meta conn))]
        (callback report))
      report)))

(defn ^:export reset-conn!
  ([conn db] (reset-conn! conn db nil))
  ([conn db tx-meta]
    (let [report (db/map->TxReport
                  { :db-before @conn
                    :db-after  db
                    :tx-data   (concat
                                 (map #(assoc % :added false) (datoms @conn :eavt))
                                 (datoms db :eavt))
                    :tx-meta   tx-meta})]
      (reset! conn db)
      (doseq [[_ callback] @(:listeners (meta conn))]
        (callback report))
      db)))

(defn ^:export listen!
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
     {:pre [(conn? conn)]}
     (swap! (:listeners (meta conn)) assoc key callback)
     key))

(defn ^:export unlisten! [conn key]
  {:pre [(conn? conn)]}
  (swap! (:listeners (meta conn)) dissoc key))


;; ----------------------------------------------------------------------------
;; define data-readers to be made available to EDN readers. in CLJS
;; they're magically available. in CLJ, data_readers.clj may or may
;; not work, but you can always simply do
;;
;;  (clojure.edn/read-string {:readers datascript/data-readers} "...")
;;

(def data-readers {'datascript/Datom db/datom-from-reader
                   'datascript/DB    db/db-from-reader})

#?(:cljs
   (doseq [[tag cb] data-readers] (cljs.reader/register-tag-parser! tag cb)))


;; Datomic compatibility layer

(def ^:private last-tempid (atom -1000000))

(defn ^:export tempid
  ([part]
    (if (= part :db.part/tx)
      :db/current-tx
      (swap! last-tempid dec)))
  ([part x]
    (if (= part :db.part/tx)
      :db/current-tx
      x)))

(defn ^:export resolve-tempid [_db tempids tempid]
  (get tempids tempid))

(defn ^:export db [conn]
  {:pre [(conn? conn)]}
  @conn)

(defn ^:export transact
  ([conn tx-data] (transact conn tx-data nil))
  ([conn tx-data tx-meta]
    {:pre [(conn? conn)]}
    (let [res (transact! conn tx-data tx-meta)]
      #?(:cljs
         (reify
           IDeref
           (-deref [_] res)
           IDerefWithTimeout
           (-deref-with-timeout [_ _ _] res)
           IPending
           (-realized? [_] true))
         :clj
         (reify
           clojure.lang.IDeref
           (deref [_] res)
           clojure.lang.IBlockingDeref
           (deref [_ _ _] res)
           clojure.lang.IPending
           (isRealized [_] true))))))

;; ersatz future without proper blocking
#?(:cljs
   (defn- future-call [f]
     (let [res      (atom nil)
           realized (atom false)]
       (js/setTimeout #(do (reset! res (f)) (reset! realized true)) 0)
       (reify
         IDeref
         (-deref [_] @res)
         IDerefWithTimeout
         (-deref-with-timeout [_ _ timeout-val] (if @realized @res timeout-val))
         IPending
         (-realized? [_] @realized)))))

(defn ^:export transact-async
  ([conn tx-data] (transact-async conn tx-data nil))
  ([conn tx-data tx-meta]
    {:pre [(conn? conn)]}
    (future-call #(transact! conn tx-data tx-meta))))

(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

#?(:cljs
  (defn- to-hex-string [n l]
    (let [s (.toString n 16)
          c (count s)]
      (cond
        (> c l) (subs s 0 l)
        (< c l) (str (apply str (repeat (- l c) "0")) s)
        :else   s))))

(defn ^:export squuid
  ([]
    (squuid #?(:clj  (System/currentTimeMillis)
               :cljs (.getTime (js/Date.)))))
  ([msec]
  #?(:clj
      (let [uuid     (UUID/randomUUID)
            time     (int (/ msec 1000))
            high     (.getMostSignificantBits uuid)
            low      (.getLeastSignificantBits uuid)
            new-high (bit-or (bit-and high 0x00000000FFFFFFFF)
                             (bit-shift-left time 32)) ]
        (UUID. new-high low))
     :cljs
       (uuid
         (str
               (-> (int (/ msec 1000))
                   (to-hex-string 8))
           "-" (-> (rand-bits 16) (to-hex-string 4))
           "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
           "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
           "-" (-> (rand-bits 16) (to-hex-string 4))
               (-> (rand-bits 16) (to-hex-string 4))
               (-> (rand-bits 16) (to-hex-string 4)))))))

(defn ^:export squuid-time-millis [uuid]
  #?(:clj (-> (.getMostSignificantBits ^UUID uuid)
              (bit-shift-right 32)
              (* 1000))
     :cljs (-> (subs (str uuid) 0 8)
               (js/parseInt 16)
               (* 1000))))
