(ns datascript.datafy
  (:require [clojure.core.protocols :as cp]
            [datascript.pull-api :as dp]
            [datascript.db :as db]
            [datascript.impl.entity :as e]))

(declare datafy-entity-seq)

(defn- navize-pulled-entity [db-val pulled-entity]
  (let [ref-attrs (:db.type/ref (:rschema db-val))
        ref-rattrs (set (map db/reverse-ref ref-attrs))
        many-attrs (:db.cardinality/many (:rschema db-val))]
    (with-meta pulled-entity
               {`cp/nav (fn [coll k v]
                          (cond
                            (or (and (many-attrs k) (ref-attrs k))
                                (ref-rattrs k))
                            (datafy-entity-seq db-val v)
                            (ref-attrs k)
                            (e/entity db-val (:db/id v))
                            :else v))})))

(defn- navize-pulled-entity-seq [db-val entities]
  (with-meta entities
             {`cp/nav (fn [coll k v]
                        (e/entity db-val (:db/id v)))}))

(defn- datafy-entity-seq [db-val entities]
  (with-meta entities
             {`cp/datafy (fn [entities] (navize-pulled-entity-seq db-val entities))}))

(extend-protocol cp/Datafiable
  datascript.impl.entity.Entity
  (datafy [this]
    (let [db (.-db this)
          ref-attrs (:db.type/ref (:rschema db))
          ref-rattrs (set (map db/reverse-ref ref-attrs))
          pull-pattern (into ["*"] ref-rattrs)]
      (navize-pulled-entity db (dp/pull db pull-pattern (:db/id this))))))
