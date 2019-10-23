(ns datascript.datafy
  (:require [clojure.core.protocols :as cp]
            [datascript.pull-api :as dp]
            [clojure.string :as str]))

(declare datafy-entity)
(declare datafy-entity-seq)

(defn- attr<->rattr [attr]
  (keyword (namespace attr)
           (if (str/starts-with? (name attr) "_")
             (subs (name attr) 1)
             (str "_" (name attr)))))

(defn- pull-pattern [ref-rattrs]
  (into ["*"] ref-rattrs))


(defn- navize-entity [db-val entity]
  (let [ref-attrs (:db.type/ref (:rschema db-val))
        ref-rattrs (set (map attr<->rattr ref-attrs))
        many-attrs (:db.cardinality/many (:rschema db-val))
        pull-pattern (into ["*"] ref-rattrs)]
    (with-meta entity
               {`cp/nav (fn [coll k v]
                          (cond
                            (or (and (many-attrs k) (ref-attrs k))
                                (ref-rattrs k))
                            (datafy-entity-seq db-val
                                               (dp/pull-many db-val pull-pattern (mapv :db/id v)))
                            (ref-attrs k)
                            (datafy-entity db-val (dp/pull db-val pull-pattern (:db/id v)))
                            :else v))})))

(defn- navize-entity-seq [db-val entities]
  (with-meta entities
             {`cp/nav (fn [coll k v]
                        (datafy-entity db-val v))}))

(defn- datafy-entity [db-val entity]
  (with-meta entity
             {`cp/datafy (fn [entity]
                           (navize-entity db-val entity))}))

(defn- datafy-entity-seq [db-val entities]
  (with-meta entities
             {`cp/datafy (fn [entities] (navize-entity-seq db-val entities))}))

(defn pull
  "Same as `datascript.core/pull` but the returned map implements `datafy/nav` (See [https://clojure.github.io/clojure/clojure.datafy-api.html](https://clojure.github.io/clojure/clojure.datafy-api.html).

  This can be used to navigate through the database following ref fields.

  Usage:
  ```
  (pull db [:ref, :name] 1)
  ; => {:db/id 1,
  ;     :name \"Child\",
  ;     :ref {:db/id 2}}
  (nav (datafy (pull db [:ref, :name] 1))
       :ref
       {:db/id 2})
  ; => {:db/id 2,
  ;     :name \"Parent\",
  ;     :_ref {:db/id 1}}
  ```
  "
  [db selector eid]
  (datafy-entity db (dp/pull db selector eid)))

(defn pull-many
  "Same as [[pull]] but accepts sequence of ids and returns sequence of maps that implements `datafy/nav`."
  [db selector eids]
  (datafy-entity-seq db (dp/pull-many db selector eids)))
