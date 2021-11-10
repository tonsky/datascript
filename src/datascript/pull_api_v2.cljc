(ns ^:no-doc datascript.pull-api-v2
  (:require
   [datascript.pull-parser-v2 :as dpp]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [cond+]])
  #?(:clj
     (:import
      [clojure.lang ISeq]
      [datascript.db Datom DB]
      [datascript.pull_parser_v2 PullAttr PullPattern])))

(declare pull-impl)

(defn- child-pattern [db pull-pattern ^PullAttr pull-attr]
  (or
    (.-pattern pull-attr)
    (cond
      (.-recursive? pull-attr)
      pull-pattern

      (.-reverse? pull-attr)
      dpp/default-pattern-ref

      (db/component? db (.-name pull-attr))
      dpp/default-pattern-component

      :else 
      dpp/default-pattern-ref)))

(defn- pull-reverse-attrs [result ^DB db ^PullPattern pull-pattern id]
  (let [component? (:db/isComponent (.-rschema db))]
    (reduce
      (fn [result #?(:clj ^PullAttr attr :cljs attr)]
        (let [name          (.-name attr)
              datoms        (db/-datoms db :avet [name id])
              child-pattern (child-pattern db pull-pattern attr)
              pulled        (if (component? name)
                              (pull-impl db child-pattern (.-e ^Datom (first datoms)))
                              (into [] (map #(pull-impl db child-pattern (.-e ^Datom %))) datoms))]
          (assoc! result (.-as attr) pulled)))
      result
      (.-reverse-attrs pull-pattern))))

(defn- pull-attrs [result ^DB db ^PullPattern pull-pattern id]
  (let [many? (:db.cardinality/many (.-rschema db))
        ref?  (:db.type/ref (.-rschema db))]
    (loop [#?(:clj ^ISeq attrs :cljs ^seq attrs) (.-attrs pull-pattern)
           #?(:clj ^ISeq datoms :cljs ^seq datoms) (db/-datoms db :eavt [id])
           result      result
           many-attr   nil
           many-values nil]
      (cond+
        (nil? attrs)
        result
        
        :let [attr ^PullAttr #?(:clj (.first attrs) :cljs (-first attrs))]
        
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

        (and (not many?) (not ref?))
        (recur attrs (#?(:clj .next :cljs -next) datoms) (assoc! result (.-as attr) (.-v datom)) nil nil)

        (and many? (not ref?))
        (recur attrs (#?(:clj .next :cljs -next) datoms) result (.-as attr) (conj! (or many-values (transient [])) (.-v datom)))
        
        :let [child-pattern (child-pattern db pull-pattern attr)]

        many?
        (recur attrs (#?(:clj .next :cljs -next) datoms) result (.-as attr) (conj! (or many-values (transient [])) (pull-impl db child-pattern (.-v datom))))
        
        :else
        (recur attrs (#?(:clj .next :cljs -next) datoms) (assoc! result (.-as attr) (pull-impl db child-pattern (.-v datom))) nil nil)))))

(defn- pull-impl [db pull-pattern id]
  (-> (transient {})
    (pull-attrs db pull-pattern id)
    (pull-reverse-attrs db pull-pattern id)
    (persistent!)))

(defn pull [db pattern id]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)]
    (pull-impl db pull-pattern id)))

(defn pull-many [db pattern ids]
  {:pre [(db/db? db)]}
  (mapv #(pull-impl db (dpp/parse-pattern db pattern) %) ids))
