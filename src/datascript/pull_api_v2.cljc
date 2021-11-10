(ns ^:no-doc datascript.pull-api-v2
  (:require
   [datascript.pull-parser-v2 :as dpp]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [cond+]])
  #?(:clj
     (:import
      [datascript.db Datom DB]
      [datascript.pull_parser_v2 PullAttr PullPattern])))

(declare pull-impl)

(defn- pull-attrs [result #?(:clj ^DB db :cljs db) attrs wildcard? id]
  (let [many? (:db.cardinality/many (.-rschema db))
        ref?  (:db.type/ref (.-rschema db))]
    (loop [#?(:clj ^clojure.lang.ISeq attrs :cljs attrs) attrs
           #?(:clj ^clojure.lang.ISeq datoms :cljs datoms) (db/-datoms db :eavt [id])
           result      result
           many-attr   nil
           many-values nil]
      (cond+
        (nil? attrs)
        result
        
        :let #?(:clj  [attr ^PullAttr (.first attrs)]
                :cljs [attr (-first attrs)])
        
        (= (.-name attr) :db/id)
        (recur (#?(:clj .next :cljs -next) attrs) datoms (assoc! result (.-as attr) id) nil nil)
        
        (nil? datoms)
        (recur (next attrs) nil result many-attr many-values)
        
        :let [^Datom datom (#?(:clj .first :cljs -first) datoms)
              cmp (compare (.-name attr) (.-a datom))]
        
        (neg? cmp)
        (recur (#?(:clj .next :cljs -next) attrs) datoms (if (some? many-attr) (assoc! result many-attr (persistent! many-values)) result) nil nil)
        
        (pos? cmp)
        (recur attrs (#?(:clj .next :cljs -next) datoms) (if (some? many-attr) (assoc! result many-attr (persistent! many-values)) result) nil nil)
        
        :let [many? (many? (.-name attr))
              ref?  (ref? (.-name attr))]

        (and many? ref?)
        (recur attrs (#?(:clj .next :cljs -next) datoms) result (.-as attr) (conj! (or many-values (transient [])) (pull-impl db (.-pattern attr) (.-v datom))))
        
        many?
        (recur attrs (#?(:clj .next :cljs -next) datoms) result (.-as attr) (conj! (or many-values (transient [])) (.-v datom)))
        
        ref?
        (recur attrs (#?(:clj .next :cljs -next) datoms) (assoc! result (.-as attr) (pull-impl db (.-pattern attr) (.-v datom))) nil nil)
        
        :else
        (recur attrs (#?(:clj .next :cljs -next) datoms) (assoc! result (.-as attr) (.-v datom)) nil nil)))))

(defn- pull-impl [db ^PullPattern pull-pattern id]
  (-> (transient {})
    (pull-attrs db (.-attrs pull-pattern) (.-wildcard? pull-pattern) id)
    (persistent!)))

(defn pull [db pattern id]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)]
    (pull-impl db pull-pattern id)))

(defn pull-many [db pattern ids]
  {:pre [(db/db? db)]}
  (mapv #(pull-impl db (dpp/parse-pattern db pattern) %) ids))
