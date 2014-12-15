(ns datascript
  (:refer-clojure :exclude [filter])
  (:require
    [datascript.core :as dc]
    [datascript.query :as dq]
    [datascript.impl.entity :as de]
    [datascript.btset :as btset]))

;; SUMMING UP

(def  q dq/q)
(def  entity de/entity)
(defn entity-db [entity] (.-db entity))
(def  touch de/touch)

(def ^:const tx0 dc/tx0)

(defn- refs [schema]
  (reduce-kv (fn [acc attr v]
               (if (= (:db/valueType v) :db.type/ref)
                 (conj acc attr)
                 acc))
             #{} schema))

(defn empty-db [& [schema]]
  (dc/map->DB {
    :schema  schema
    :eavt    (btset/btset-by dc/cmp-datoms-eavt) 
    :aevt    (btset/btset-by dc/cmp-datoms-aevt)
    :avet    (btset/btset-by dc/cmp-datoms-avet)
    :max-eid 0
    :max-tx  tx0
    :refs    (refs schema)}))

(defn init-db [datoms & [schema]]
  (let [datoms  (into-array datoms)
        len     (alength datoms)
        eavt    (btset/-btset-from-sorted-arr (.sort datoms dc/cmp-datoms-eavt-quick) dc/cmp-datoms-eavt)
        max-eid (if (pos? len) (.-e (aget datoms (dec len))) 0)
        aevt    (btset/-btset-from-sorted-arr (.sort datoms dc/cmp-datoms-aevt-quick) dc/cmp-datoms-aevt)
        avet    (btset/-btset-from-sorted-arr (.sort datoms dc/cmp-datoms-avet-quick) dc/cmp-datoms-avet)
        max-tx  (transduce (map #(.-tx %)) max tx0 datoms)]
    (dc/map->DB {
      :schema  schema
      :eavt    eavt
      :aevt    aevt
      :avet    avet
      :max-eid max-eid
      :max-tx  max-tx
      :refs    (refs schema)})))

(defn is-filtered [db]
  (instance? dc/FilteredDB db))

(defn filter [db pred]
  (if (is-filtered db)
    (let [u (.-unfiltered-db db)]
      (dc/FilteredDB. u #(and (pred u %) ((.-pred db) %))))
    (dc/FilteredDB. db #(pred db %))))

(defn with [db tx-data & [tx-meta]]
  (if (is-filtered db)
    (throw (js/Error. "Filtered DB cannot be modified"))
    (dc/transact-tx-data (dc/map->TxReport
                           { :db-before db
                             :db-after  db
                             :tx-data   []
                             :tempids   {}
                             :tx-meta   tx-meta}) tx-data)))

(defn db-with [db tx-data]
  (:db-after (with db tx-data)))

(defn datoms [db index & cs]
  (dc/-datoms db index cs))

(defn seek-datoms [db index & cs]
  (dc/-seek-datoms db index cs))

(defn index-range [db attr start end]
  (dc/-index-range db attr start end))

;; Conn

(defn create-conn [& [schema]]
  (atom (empty-db schema)
        :meta { :listeners  (atom {}) }))

(defn -transact! [conn tx-data tx-meta]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with db tx-data tx-meta)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact! [conn tx-data & [tx-meta]]
  (let [report (-transact! conn tx-data tx-meta)]
    (doseq [[_ callback] @(:listeners (meta conn))]
      (callback report))
    report))
           
(defn listen!
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
     (swap! (:listeners (meta conn)) assoc key callback)
     key))

(defn unlisten! [conn key]
  (swap! (:listeners (meta conn)) dissoc key))


;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(extend-type dc/Datom
  IPrintWithWriter
  (-pr-writer [d w opts]
    (pr-sequential-writer w pr-writer "#datascript/Datom [" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)])))

(defn datom [e a v & [tx added]]
  (dc/Datom. e a v (or tx tx0) (if (nil? added) true added)))

(defn datom-from-reader [[e a v tx added]]
  (datom e a v tx added))

(defn pr-db [db w opts]
  (-write w "#datascript/DB {")
  (-write w ":schema ")
  (pr-writer (dc/-schema db) w opts)
  (-write w ", :datoms ")
  (pr-sequential-writer w
    (fn [d w opts]
      (pr-sequential-writer w pr-writer "[" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d)]))
    "[" " " "]" opts (dc/-datoms db :eavt []))
  (-write w "}"))

(extend-protocol IPrintWithWriter
  dc/DB
  (-pr-writer [db w opts]
    (pr-db db w opts))

  dc/FilteredDB
  (-pr-writer [db w opts]
    (pr-db db w opts)))

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (dc/Datom. e a v tx true)) datoms) schema))


;; Datomic compatibility layer

(def last-tempid (atom -1000000))

(defn tempid
  ([part]
    (if (= part :db.part/tx)
      :db/current-tx
      (swap! last-tempid dec)))
  ([part x]
    (if (= part :db.part/tx)
      :db/current-tx
      x)))

(defn resolve-tempid [_db tempids tempid]
  (get tempids tempid))

(def db deref)

(defn transact [conn tx-data & [tx-meta]]
  (let [res (transact! conn tx-data tx-meta)]
    (reify
      IDeref
      (-deref [_] res)
      IDerefWithTimeout
      (-deref-with-timeout [_ _ _] res)
      IPending
      (-realized? [_] true))))

;; ersatz future without proper blocking
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
      (-realized? [_] @realized))))

(defn transact-async [conn tx-data & [tx-meta]]
  (future-call #(transact! conn tx-data tx-meta)))

(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

(defn squuid []
  (UUID.
    (str
          (-> (js/Date.) (.getTime) (/ 1000) (Math/round) (.toString 16))
      "-" (-> (rand-bits 16) (.toString 16))
      "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (.toString 16))
      "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (.toString 16))
      "-" (-> (rand-bits 16) (.toString 16))
          (-> (rand-bits 16) (.toString 16))
          (-> (rand-bits 16) (.toString 16)))))

(defn squuid-time-millis [uuid]
  (-> (subs (.-uuid uuid) 0 8)
      (js/parseInt 16)
      (* 1000)))
