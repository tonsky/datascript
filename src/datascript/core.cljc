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

(def
  ^{:arglists '([q & inputs])
    :doc "Execute a datalog query.
         (For full docs see Datomic reference: https://docs.datomic.com/query.html)

         Usage:
         (q '[:find ?value :where [_ :likes ?value]] db)
         -> #{[:pizza]}"}
  q dq/q)
(def ^{:arglists '([db eid])
       :doc "Retrieves a single entity by its id from the given database.
            Retrieve using the internal db id:
            (entity db 1)
            Retrieve using a user defined id:
            (entity db [:db/ident :a-unique-key])"}
  entity de/entity)

(defn entity-db [^Entity entity]
  {:pre [(de/entity? entity)]}
  (.-db entity))

(def ^{:arglists '([e a v] [e a v tx] [e a v tx added])
       :doc "Creates a single datom that can be added to any database.
             A datom consists of [entity-id attribute value] where:
             - entity-id is the entity's unique id within the db
             - attribute is the relation that the entity has to the value
             - value is the value that the attribute applies to.
             This can be a primitive value or refer to another entity.

             For example:
             To asserts a single fact:
             (datom [1 :likes :pizza])
             To assert with transaction data:
             (datom [1 :likes :pizza 17])
             Add true to assert the fact (default), false to retract it:
             (datom [1 :likes :pizza 17 false])"}
  datom db/datom)
(def ^{:arglists '([db selector eid])
       :doc "Pull a single entity from the database using Datomic pull syntax.
            (For full docs see Datom reference: http://docs.datomic.com/pull.html)

            Usage:
            (pull db [:likes :dislikes] 1)
            -> {:likes :pizza :dislikes :ice-cream}"}
  pull dp/pull)
(def ^{:arglists '([db selector eids])
       :doc "Pull entities from the database using Datomic pull syntax.
            (For full docs see Datomic reference: http://docs.datomic.com/pull.html)

            Usage:
            (pull-many db [:likes :dislikes] [1 2])
            -> [{:likes :pizza :dislikes :ice-cream} {:likes :pie}]"}
  pull-many dp/pull-many)
(def ^{:arglists '([e])
       :doc "Retrieves all attributes for a given entity as a single map.

            Usage:
            (touch (entity db 1)) ; -> {:dislikes :pie, :likes :pizza, :db/id 1}"}
  touch de/touch)
(def ^{:arglists '([] [schema])
       :doc "Creates an empty database with an optional schema.

            Usage:
            (empty-db)
            With schema:
            (empty-db {:likes {:db/cardinality :db.cardinality/many}})"}
  empty-db db/empty-db)
(def ^{:arglists '([datoms] [datoms schema])
       :doc "Creates a database from a given collection of datoms.
            Takes an optional schema.

            Usage:
            (init-db
              [(datom 1 :likes :pizza)])
            With a schema:
            (init-db
              [(datom 1 :likes :pizza)]
              {:likes {:db/cardinality :db.cardinality/many}})"}
  init-db db/init-db)
(def ^{:arglists '([x])
       :doc "Returns true if the given value is a datom, false otherwise."}
  datom? db/datom?)
(def ^{:arglists '([x])
       :doc "Return true if the given value is a database
            (but not a database connection), false otherwise."}
  db? db/db?)
(def ^:const tx0 db/tx0)

(defn is-filtered
  "Returns true if this is an instance of a filtered database, false otherwise."
  [x]
  (instance? FilteredDB x))

(defn filter
  "Returns a database including only the datoms for which the (pred db datom)
  holds true."
  [db pred]
  {:pre [(db/db? db)]}
  (if (is-filtered db)
    (let [^FilteredDB fdb db
          orig-pred (.-pred fdb)
          orig-db   (.-unfiltered-db fdb)]
      (FilteredDB. orig-db #(and (orig-pred %) (pred orig-db %)) (atom 0)))
    (FilteredDB. db #(pred db %) (atom 0))))

(defn with
  "Apply the given transaction to the given database returning a
  transaction report map with :db-before, :db-after, :tx-data, :tempids
  and :tx-meta.
  Note that this function has no side-effects and the given database
  remains untouched."
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

(defn db-with
  "Same as (with db tx-data) but will only return the new database
  (i.e. :db-after)."
  [db tx-data]
  {:pre [(db/db? db)]}
  (:db-after (with db tx-data)))

(defn datoms
  ([db index]             {:pre [(db/db? db)]} (db/-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3 c4])))

(defn seek-datoms
  ([db index]             {:pre [(db/db? db)]} (db/-seek-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-seek-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3 c4])))

(defn rseek-datoms
  "Iterates index from given point backwards, until the beginning of the index"
  ([db index]             {:pre [(db/db? db)]} (db/-rseek-datoms db index []))
  ([db index c1]          {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1]))
  ([db index c1 c2]       {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2]))
  ([db index c1 c2 c3]    {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2 c3 c4])))

(defn index-range [db attr start end]
  {:pre [(db/db? db)]}
  (db/-index-range db attr start end))

(def ^{:arglists '([db eid])} entid db/entid)

;; Conn

(defn conn?
  "Returns true if this is a connection to a db, false otherwise."
  [conn]
  (and #?(:clj  (instance? clojure.lang.IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
    (db/db? @conn)))

(defn conn-from-db
  "Given a database creates a connection to said database."
  [db]
  (atom db :meta { :listeners (atom {}) }))

(defn conn-from-datoms
  "Creates a database for the given datoms and returns a connection
  to said database. Takes an optional schema."
  ([datoms]        (conn-from-db (init-db datoms)))
  ([datoms schema] (conn-from-db (init-db datoms schema))))

(defn create-conn
  "Creates a connection to an empty database.
  Takes an optional schema."
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

(defn transact!
  "Applies the given transaction data to the database connection.
  The database under the connection now refers to the updated
  database that has the transaction applied to it.
  The result of the transaction is returned containing report map of
  :db-before, :db-after, :tx-data, :tempids and :tx-meta.

  Usage:
  (transact! conn1 [{:db/id -1 :likes :pizza}])"
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
    {:pre [(conn? conn)]}
    (let [report (-transact! conn tx-data tx-meta)]
      (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
        (callback report))
      report)))

(defn reset-conn!
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
      (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
        (callback report))
      db)))

(defn- atom? [a]
  #?(:cljs (instance? Atom a)
     :clj  (instance? clojure.lang.IAtom a)))

(defn listen!
  "Listen for changes on the given database connection.
  Whenever a transaction is applied to the database the callback is called
  with the transaction report.
  Returns the key under which this listener is registered."
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
     {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
     (swap! (:listeners (meta conn)) assoc key callback)
     key))

(defn unlisten!
  "Removes the listener under de given key from the database connection."
  [conn key]
  {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
  (swap! (:listeners (meta conn)) dissoc key))


;; ----------------------------------------------------------------------------
;; define data-readers to be made available to EDN readers. in CLJS
;; they're magically available. in CLJ, data_readers.clj may or may
;; not work, but you can always simply do
;;
;;  (clojure.edn/read-string {:readers datascript/data-readers} "...")
;;

(def ^{:doc "data-readers available for parsing datascript values with
            clojure.edn/read-string.
            Usage: (clojure.edn/read-string {:readers datascript/data-readers} \"...\")"}
  data-readers {'datascript/Datom db/datom-from-reader
                'datascript/DB    db/db-from-reader})

#?(:cljs
   (doseq [[tag cb] data-readers] (cljs.reader/register-tag-parser! tag cb)))


;; Datomic compatibility layer

(def ^:private last-tempid (atom -1000000))

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

(defn db
  "Extracts the current database (as a value) from the connection."
  [conn]
  {:pre [(conn? conn)]}
  @conn)

(defn transact
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

(defn transact-async
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

(defn squuid
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

(defn squuid-time-millis [uuid]
  #?(:clj (-> (.getMostSignificantBits ^UUID uuid)
              (bit-shift-right 32)
              (* 1000))
     :cljs (-> (subs (str uuid) 0 8)
               (js/parseInt 16)
               (* 1000))))
