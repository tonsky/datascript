(ns datascript.extensions
  (:require [datascript :as d]))

; Start with a high ID so that we can distinguish records newly created from ones loaded
; from a source database that uses sequential IDs.
(def min-new-id 0x50000000)

(let [tempid-source (atom 0)]
  (defn next-tempid
    "A simple source of negative tempids for new entities."
    []
    (swap! tempid-source dec))
  (defn reset-tempids
    "For testing to enable predictable tempids"
    []
    (reset! tempid-source 0)))

(defn create-conn
  "adjust the db with our higher starting ID and a vector for history datoms
   (see transact! below). The ident key is set with an arbitrary gensym so that
   versions of a database can be distinguished reliably"
  [schema metadata]
  (let [conn (d/create-conn schema)]
    (atom (assoc @conn
                 :max-eid min-new-id
                 :history []
                 :ident (gensym "DBv"))
          :meta (merge metadata (meta conn)))))

(defn merge-schema
  "Can't change existing schema keys, but can add new ones"
  [db to-merge]
  (d/map->DB
    (assoc (into {} db)
           :schema (merge to-merge
                          (:schema db)))))

(defn with-many-rel
  "Add many rels to a db schema"
  [db & rels]
  (merge-schema
    db
    (reduce (fn [m r]
              (assoc m r {:db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/many}))
            {} (flatten rels))))

(defn with-one-rel
  "Add one rels to a db schema"
  [db & rels]
  (merge-schema
    db
    (reduce (fn [m r]
              (assoc m r {:db/valueType :db.type/ref}))
            {} (flatten rels))))

(defprotocol IEntity
  (-entity [e])
  (-entity-id [e]))

(defn entity?
  "True if e is an entity"
  [e]
  (satisfies? IEntity e))

(defn entity-id
  "Given an entity return its id, or given an integer, return it"
  [e]
  (cond (entity? e) (-entity-id e)
        (integer? e) e))

(defn tempid?
  "Is this entity id an unresolved tempid?"
  [e]
  (when-let [e (entity-id e)] (neg? e)))

(defn explode
  "Recursively explodes plain maps into a list of commands.

   - Prepend changed multival attributes with db.fn/retractAttribute (unless first-load? flag is set)
   - Adds Entity relationships without recursively expanding them
   - Handles string keys (useful for importing raw JSON as read by transit)
   - Skips namespaced attribute keys
   - Uses next-tempid if the map has no specified id
   - Retracts any attribute with a nil value"
  [db data {:keys [::seen first-load?] :as opts}]
  (cond
    (vector? data) [data]
    (entity? data) []
    (and (map? data) (not (get seen data)))
    (let [eid (or (:db/id data) (:id data) (get data "id") (next-tempid))
          seen (if seen (conj seen data) #{data})
          commands
          (for [[a vs] (dissoc data :db/id :id "id")
                :let [a (or (keyword a)
                            (throw (ex-info (str "Invalid attribute key: " (pr-str a) " in record " eid)
                                            {:a a :data data})))]
                :when (not (namespace a))
                v        (if (and (d/multival? db a) (sequential? vs)) vs [vs])
                command  (if (d/ref? db a)
                           (cond
                             (entity? v)
                             [[:db/add eid a (-entity-id v)]]
                             (map? v)
                             (let [[[_ rel-eid] :as commands] (explode db v (assoc opts ::seen seen))]
                               (if rel-eid
                                 (conj commands [:db/add eid a rel-eid])
                                 [nil]))
                             :else [nil])
                           [nil])
                command (cond
                          command [command]
                          (some? v) [[:db/add eid a v]]
                          first-load? []
                          (not first-load?) [[:db.fn/retractAttribute eid a]])]
            command)]
      (if first-load?
        commands
        (concat
          (->> commands
               (filter (fn [[op eid a]]
                         (and (= :db/add op) (pos? eid) (d/multival? db a))))
               (map #(subvec % 0 3))
               set
               (map (fn [[_ eid a]]
                      [:db.fn/retractAttribute eid a])))
          commands)))))

(defn explode-all
  "explodes each entity using the explode fn in this ns"
  [db entities opts]
  (mapcat #(explode db % opts) entities))

(defprotocol ICommands
  (-with-commands
    [e commands f args]
    "Return a new instance with the given commands and having applied the given f")
  (-get-commands
    [e seen]
    "Recursively get all commands on the given entity and recursively from all entities under it."))

(defn- with-commands [e c f & args]
  (-with-commands e c f args))

(defn get-commands
  "Get all datascript commands to update the db to match this entity and its children"
  [e]
  (when (entity? e)
    (-get-commands e (atom #{}))))

(defn transact!
  "Execute the operations contained in an arbitrary list of datascript command
   operations, uncommitted entity changes, and raw maps of data against the given
   datascript connection.

   Options:
   - explode-all: a fn matching the signature of explode-all to inject an alternate strategy
   - first-load?: set it to true and no db.fn/retract* calls will be generated"
  [conn records & {:keys [explode-all] :as opts :or {explode-all explode-all}}]
  (let [seen (atom #{})
        records (mapcat #(if (entity? %) (-get-commands % seen) [%]) records)
        new-version (gensym "DBv")
        report (assoc-in (d/transact! conn (explode-all @conn records (into {} opts)))
                         [:db-after :ident] new-version)]
    ; TODO: consider saving all past tempids so they can be resolved against any future db...
    (swap! conn #(-> %
                     (assoc :ident new-version)
                     (update-in [:history] (comp vec concat) (:tx-data report))))
    report))

(defprotocol ICache
  (-cache
    [e cache] [e k v] [e k v return-value]
    "Arity 2: merge a map of data into the cache. Arity 3, cache k as v and
     return v. Arity 4 allows an alternate return value, useful for caching
     not-found keys"))

(defprotocol IFindEntity
  (-find-entity [o e]))

(defn entity
  "Find an existing entity given an id or an Entity.

   Can be called against either a DB or a TxReport. When a TxReport, will
   resolve tempids."
  ([o e]
   (when-let [e (-find-entity o e)]
     (when (seq e) e)))
  ([o e attrs]
   (when-let [e (-find-entity o e)]
     (into e attrs))))

(deftype Entity [db eid datoms ^:mutable cache commands]
  Object
  (toString [coll]
    (pr-str* coll))
  (equiv [this other]
    (-equiv this other))

  ;; Experimental stuff copied from PersistentHashMap for JS Map iface.
  (keys [coll]
    (iterator (keys coll)))
  (entries [coll]
    (entries-iterator (seq coll)))
  (values [coll]
    (iterator (vals coll)))
  (has [coll k]
    (contains? coll (keyword k)))
  (get [coll k]
    (-lookup coll (keyword k)))
  (forEach [coll f]
    (doseq [[k v] coll]
      (f v k)))

  IEquiv
  (-equiv [coll other]
    (cond (instance? Entity other)
          (and (= eid (-entity-id other))
               (= db (.-db other))
               (= commands (.-commands other)))))

  IHash
  (-hash [coll] (hash [db eid commands]))

  ISeqable
  (-seq [coll]
    (when (pos? (count coll))
      (map (fn [d] [(.-a d) (-lookup coll (.-a d))]) datoms)))

  ICounted
  ; This may not be accurate if the record has been changed.
  ; TODO: maintain a count delta from -assoc
  (-count [coll] (count datoms))

  ILookup
  (-lookup [o k]
    (-lookup o k nil))
  (-lookup [o k not-found]
    (cond
      (contains? cache k) (let [v (cache k)]
                               (if (= ::not-found v) not-found v))
      (or (= :id k) (= :db/id k)) eid
      (d/multival? db k)
      (let [value (reduce (if (d/ref? db k)
                            (fn [value datom]
                              (if-let [ent (entity db (.-v datom))]
                                (conj value ent)
                                value))
                            (fn [value datom]
                              (conj value (.-v datom))))
                          []
                          (filter #(= k (.-a %)) datoms))]
        (if (empty? value)
          (-cache o k ::not-found not-found)
          (-cache o k value)))
      :else
      (loop [[datom & ds] datoms]
        (cond
          (nil? datom) (-cache o k ::not-found not-found)
          (= k (.-a datom)) (-cache o k (if (d/ref? db k)
                                            (entity db (.-v datom))
                                            (.-v datom)))
          :else (recur ds)))))

  ICollection
  ; Identical to PersistentHashMap
  (-conj [coll entry]
    (if (vector? entry)
      (-assoc coll (-nth entry 0) (-nth entry 1))
      (loop [ret coll es (seq entry)]
        (if (nil? es)
          ret
          (let [e (first es)]
            (if (vector? e)
              (recur (-assoc ret (-nth e 0) (-nth e 1))
                     (next es))
              (throw (js/Error. "conj on a map takes map entries or seqables of map entries"))))))))

  ICommands
  (-with-commands [coll cmds f args]
    (Entity. db eid datoms (apply f cache args) (into commands cmds)))
  (-get-commands [coll seen]
    (when-not (@seen coll)
      (swap! seen conj coll)
      (concat
        (filter vector? commands)
        (->> commands
             (filter map?)
             (group-by ::key)
             vals
             (map (comp #(dissoc % ::key) last)))
        (->> cache
             vals
             flatten
             (filter entity?)
             (mapcat #(-get-commands % seen))))))

  IAssociative
  (-assoc [coll k v]
    (letfn [(assert-same-db [e]
              (when (and (tempid? e)
                         (not= (:ident (.-db coll)) (:ident (.-db e))))
                (throw (ex-info (str "Can not assoc new entities from a previous transaction. Resolve new entities with (entity tx-report e).")
                                {:target coll :new-entity e}))))]
      (cond
        (nil? k) coll
        (and (d/ref? db k) (d/multival? db k))
        (if (every? entity? v)
          (do (doseq [e v] (assert-same-db e))
              (with-commands coll [{:db/id eid k (when (seq v) (vec v))}]
                assoc k (if (seq v) (vec v) ::not-found)))
          (throw (ex-info (str "Can only assoc a collection of entities to attr " k)
                          {:entity coll :k k :v v ::key k})))
        (d/ref? db k)
        (if (or (nil? v) (entity? v))
          (do (when v (assert-same-db v))
              (with-commands coll [{:db/id eid k v ::key k}]
                assoc k (or v ::not-found)))
          (throw (ex-info (str "Can only assoc an entity to attr " k)
                          {:entity coll :k k :v v})))
        :else
        (with-commands coll [{:db/id eid k v ::key k}]
          assoc k (if (nil? v) ::not-found v)))))
  (-contains-key? [coll k]
    (if (nil? k) false
      (not= ::nf (-lookup coll k ::nf))))

  IMap
  (-dissoc [coll k]
    (-assoc coll k nil))

  IEntity
  (-entity [e] e)
  (-entity-id [e] eid)

  ICache
  (-cache [e res]
    (set! cache (merge res cache)))
  (-cache [e k v]
    (-cache e k v v))
  (-cache [e k v return-value]
    (set! cache (assoc cache k v))
    return-value)

  IFn
  (-invoke [coll k]
    (-lookup coll k))

  (-invoke [coll k not-found]
    (-lookup coll k not-found))

  IPrintWithWriter
  (-pr-writer [o writer _]
    (-write writer (str "#db/entity " (pr-str (into {:db/id eid} (remove #(= ::not-found (val %)) cache)))))))

(extend-protocol IFindEntity
  d/DB
  (-find-entity [db e]
    (when-let [e (entity-id e)]
      (when-let [datoms (not-empty (d/-search db [e]))]
        (Entity. db e datoms nil []))))

  d/TxReport
  (-find-entity [db e]
    (when-let [e (entity-id e)]
      (entity (:db-after db) (get (:tempids db) e e)))))

(defn new-entity
  "Create a new entity in the database"
  ([db]
   (Entity. db (next-tempid) [] nil []))
  ([db attrs]
   (into (new-entity db) attrs)))

(defn delete-entity
  "The entity will only be deleted if the returned entity is included in a call to transact!"
  ([e]
   (when (entity? e)
     (delete-entity (.-db e) e)))
  ([db e]
   (when-let [e (entity-id e)]
     (Entity. db e [] {::deleted true} [[:db.fn/retractEntity e]]))))

(defn delete?
  "Is this entity marked for deletion?"
  [e]
  (when (entity? e)
    (::deleted (.-cache e))))

(defn touch
  "Resolve all attributes for an entity. Not generally needed, but useful for debugging."
  [e]
  (dorun (seq e))
  e)

(defn history
  "Get all history datoms for the db."
  [db]
  (:history db))

(defn created?
  "Is this entity newly created with an auto-generated id?"
  [e]
  (when-let [id (entity-id e)]
    (<= min-new-id id)))
