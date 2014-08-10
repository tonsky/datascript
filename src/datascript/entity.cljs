(ns datascript.entity
  (:require [datascript :as d]))

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

(defprotocol ICache
  (-cache
    [e k v] [e k v return-value]
    "Arity 3, cache k as v and return v.
     Arity 4 allows an alternate return value, useful for caching not-found keys"))

(defprotocol IFindEntity
  (-find-entity [o e]))

(defn entity
  "Find an existing entity given an id or an Entity.

   Can be called against either a DB or a TxReport. When a TxReport, will
   resolve tempids."
  [o e]
  (-find-entity o e))

(deftype Entity [db eid datoms ^:mutable cache]
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
               (= db (.-db other)))))

  IHash
  (-hash [coll] (hash [db eid]))

  ISeqable
  (-seq [coll]
    (when (pos? (count coll))
      (map (fn [d] [(.-a d) (-lookup coll (.-a d))]) datoms)))

  ICounted
  (-count [coll] (count (seq coll)))

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

  IAssociative
  (-contains-key? [coll k]
    (if (nil? k) false
      (not= ::nf (-lookup coll k ::nf))))

  IEntity
  (-entity [e] e)
  (-entity-id [e] eid)

  ICache
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
        (Entity. db e datoms nil))))

  d/TxReport
  (-find-entity [db e]
    (when-let [e (entity-id e)]
      (entity (:db-after db) (get (:tempids db) e e)))))

(defn touch
  "Resolve all attributes for an entity. Not generally needed, but useful for debugging."
  [e]
  (dorun (seq e))
  e)
