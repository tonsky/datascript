(ns datascript
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

(def ^:const tx0 0x20000000)

(defn- refs [schema]
  (->> schema
    (filter (fn [[_ v]] (= (:db/valueType v) :db.type/ref)))
    (mapv first)))

(defn empty-db [& [schema]]
  (dc/map->DB {
    :schema  schema
    :eavt    (btset/btset-by dc/cmp-datoms-eavt) 
    :aevt    (btset/btset-by dc/cmp-datoms-aevt)
    :avet    (btset/btset-by dc/cmp-datoms-avet)
    :max-eid 0
    :max-tx  tx0
    :refs    (refs schema)}))

(defn create-conn [& [schema]]
  (atom (empty-db schema)
        :meta { :listeners  (atom {}) }))

(defn with [db tx-data]
  (dc/transact-tx-data (dc/TxReport. db db [] {}) tx-data))

(defn db-with [db tx-data]
  (:db-after (with db tx-data)))

(defn -transact! [conn tx-data]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with db tx-data)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact! [conn tx-data]
  (let [report (-transact! conn tx-data)]
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

(defn- components->pattern [index [c0 c1 c2 c3]]
  (case index
    :eavt (dc/Datom. c0 c1 c2 c3 nil)
    :aevt (dc/Datom. c1 c0 c2 c3 nil)
    :avet (dc/Datom. c2 c0 c1 c3 nil)))

(defn datoms [db index & cs]
  (btset/slice (get db index) (components->pattern index cs)))

(defn seek-datoms [db index & cs]
  (btset/slice (get db index) (components->pattern index cs) (dc/Datom. nil nil nil nil nil)))

(defn index-range [db attr start end]
  (btset/slice (:avet db) (dc/Datom. nil attr start nil nil)
                          (dc/Datom. nil attr end nil nil)))

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(extend-type dc/Datom
  IPrintWithWriter
  (-pr-writer [d w opts]
    (pr-sequential-writer w pr-writer "#datascript/Datom [" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)])))

(defn datom-from-reader [[e a v tx added]]
  (dc/Datom. e a v tx added))

(extend-type dc/DB
  IPrintWithWriter
  (-pr-writer [db w opts]
    (-write w "#datascript/DB {")
    (-write w ":schema ")
    (pr-writer (.-schema db) w opts)
    (-write w ", :datoms ")
    (pr-sequential-writer w
      (fn [d w opts]
        (pr-sequential-writer w pr-writer "[" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d)]))
      "[" " " "]" opts (.-eavt db))
    (-write w "}")))

(defn db-from-reader [{:keys [schema datoms]}]
  (let [datoms (map (fn [[e a v tx]] (dc/Datom. e a v tx true)) datoms)]
    (dc/map->DB {
      :schema  schema
      :eavt    (apply btset/btset-by dc/cmp-datoms-eavt datoms)
      :aevt    (apply btset/btset-by dc/cmp-datoms-aevt datoms)
      :avet    (apply btset/btset-by dc/cmp-datoms-avet datoms)
      :max-eid (reduce max 0 (map :e datoms))
      :max-tx  (reduce max tx0 (map :tx datoms))
      :refs    (refs schema)})))


;; Datomic compatibility layer

(def last-tempid (atom -1000000))
(defn tempid
  ([_part]  (swap! last-tempid dec))
  ([_part x] x))
(defn resolve-tempid [_db tempids tempid]
  (get tempids tempid))

(def db deref)

(defn transact [conn tx-data]
  (let [res (transact! conn tx-data)]
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

(defn transact-async [conn tx-data]
  (future-call #(transact! conn tx-data)))

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
