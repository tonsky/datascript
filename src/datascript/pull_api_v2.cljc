(ns ^:no-doc datascript.pull-api-v2
  (:require
    [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [cond+]])
  #?(:clj (:import [datascript.db Datom])))

(defn pull
  "WIP: Selector is just a list of attrs"
  [db selector id]
  (let [attrs (list* (sort selector))
        many? (:db.cardinality/many (.-rschema db))
        ref?  (:db.type/ref (.-rschema db))
        impl  (fn impl [db id]
                (loop [#?(:clj ^clojure.lang.ISeq attrs :cljs attrs) attrs
                       #?(:clj ^clojure.lang.ISeq datoms :cljs datoms) (db/-datoms db :eavt [id])
                       result      (transient {})
                       many-attr   nil
                       many-values nil]
                  (cond+
                    (nil? datoms)
                    (persistent! result)
                    
                    (nil? attrs)
                    (persistent! result)
                    
                    :let [attr (#?(:clj .first :cljs -first) attrs)]
                    
                    (= attr :db/id)
                    (recur (#?(:clj .next :cljs -next) attrs) datoms (assoc! result :db/id id) nil nil)
                    
                    :let [^Datom datom (#?(:clj .first :cljs -first) datoms)
                          cmp (compare attr (.-a datom))]
                    
                    (neg? cmp)
                    (recur (#?(:clj .next :cljs -next) attrs) datoms (if (some? many-attr) (assoc! result many-attr (persistent! many-values)) result) nil nil)
                    
                    (pos? cmp)
                    (recur attrs (#?(:clj .next :cljs -next) datoms) (if (some? many-attr) (assoc! result many-attr (persistent! many-values)) result) nil nil)
                    
                    (and (many? attr) (ref? attr))
                    (recur attrs (#?(:clj .next :cljs -next) datoms) result attr (conj! (or many-values (transient [])) (impl db (.-v datom))))
                    
                    (many? attr)
                    (recur attrs (#?(:clj .next :cljs -next) datoms) result attr (conj! (or many-values (transient [])) (.-v datom)))
                    
                    (ref? attr)
                    (recur attrs (#?(:clj .next :cljs -next) datoms) (assoc! result attr (impl db (.-v datom))) nil nil)
                    
                    :else
                    (recur attrs (#?(:clj .next :cljs -next) datoms) (assoc! result attr (.-v datom)) nil nil))))]
      (impl db id)))
