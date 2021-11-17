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

(defn- first-seq [#?(:clj ^ISeq xs :cljs ^seq xs)]
  (when (some? xs)
    #?(:clj (.first xs) :cljs (-first xs))))

(defn- next-seq [#?(:clj ^ISeq xs :cljs ^seq xs)]
  (when (some? xs)
    #?(:clj (.next xs) :cljs (-next xs))))

(defn- pull-reverse-attrs [result db ^PullPattern pull-pattern id seen]
  (let [component? (:db/isComponent (:rschema db))]
    (reduce
      (fn [result ^PullAttr attr]
        (let [name          (.-name attr)
              datoms        (db/-datoms db :avet [name id])
              child-pattern (if (.-recursive? attr) pull-pattern (.-pattern attr))
              pulled        (if (component? name)
                              (pull-impl db child-pattern (.-e ^Datom (first datoms)) seen)
                              (into [] (map #(pull-impl db child-pattern (.-e ^Datom %) seen)) datoms))]
          (assoc! result (.-as attr) pulled)))
      result
      (.-reverse-attrs pull-pattern))))

(defn- pull-multival-attr [^PullAttr attr datoms]
  (loop [acc    (transient [])
         datoms datoms
         count  0]
    (cond+
      (nil? datoms)
      [(persistent! acc) datoms]

      :let [^Datom datom (first-seq datoms)]

      (not= (.-a datom) (.-name attr))
      [(persistent! acc) datoms]

      (and (.-limit attr) (>= count (.-limit attr)))
      (recur acc (next datoms) count)

      :else
      (recur (conj! acc (.-v datom)) (next-seq datoms) (inc count)))))

(defn- pull-multival-ref-attr [^PullAttr attr datoms db child-pattern seen]
  (loop [acc    (transient [])
         datoms datoms
         count  0]
    (cond+
      (nil? datoms)
      [(persistent! acc) datoms]

      :let [^Datom datom (first-seq datoms)]

      (not= (.-a datom) (.-name attr))
      [(persistent! acc) datoms]

      (and (.-limit attr) (>= count (.-limit attr)))
      (recur acc (next datoms) count)

      :let [id  (.-v datom)
            val (pull-impl db child-pattern id seen)]

      :else
      (recur (conj! acc val) (next-seq datoms) (inc count)))))

(defn- pull-attrs [result db ^PullPattern pull-pattern id seen]
  (let [many?      (:db.cardinality/many (:rschema db))
        ref?       (:db.type/ref (:rschema db))
        component? (:db/isComponent (:rschema db))
        attrs      (.-attrs pull-pattern)]
    (loop [^PullAttr attr (first-seq attrs)
           attrs          (next-seq attrs)
           datoms         (db/-datoms db :eavt [id])
           result         result]
      ; (prn id (:name attr) (first-seq datoms))
      (cond+
        (and (nil? attr) (.-wildcard? pull-pattern) datoms)
        (recur
          (dpp/parse-attr-name db (.-a ^Datom (first-seq datoms)))
          attrs
          datoms
          result)

        (nil? attr)
        result

        (= (.-name attr) :db/id)
        (recur
          (first-seq attrs)
          (next-seq attrs)
          datoms
          (assoc! result (.-as attr) id))
        
        (nil? datoms)
        (recur
          (first-seq attrs)
          (next-seq attrs)
          datoms
          result)
        
        :let [^Datom datom (first-seq datoms)
              cmp (compare (.-name attr) (.-a datom))]
        
        (and (pos? cmp) (.-wildcard? pull-pattern))
        (recur
          (dpp/parse-attr-name db (.-a datom))
          attrs
          datoms
          result)

        (neg? cmp)
        (recur 
          (first-seq attrs)
          (next-seq attrs)
          datoms
          result)
        
        (pos? cmp)
        (recur
          attr
          attrs
          (next-seq datoms)
          result)
        
        :let [many? (many? (.-name attr))
              ref?  (ref? (.-name attr))]

        (and (not many?) (not ref?))
        (recur attr attrs (next-seq datoms) (assoc! result (.-as attr) (.-v datom)))

        (and many? (not ref?))
        (let [[vals datoms'] (pull-multival-attr attr datoms)]
          (recur
            (first-seq attrs)
            (next-seq attrs)
            datoms'
            (assoc! result (.-as attr) vals)))

        :let [child-pattern (if (.-recursive? attr) pull-pattern (.-pattern attr))]

        many?
        (let [[vals datoms'] (pull-multival-ref-attr attr datoms db child-pattern seen)]
          (recur
            (first-seq attrs)
            (next-seq attrs)
            datoms'
            (assoc! result (.-as attr) vals)))

        (not many?)
        (recur
          attr
          attrs
          (next-seq datoms)
          (assoc! result (.-as attr) (pull-impl db child-pattern (.-v datom) seen)))))))

(defn- pull-impl [db ^PullPattern pull-pattern id #?(:clj ^java.util.Set seen :cljs seen)]
  (if #?(:clj  (.contains seen id) :cljs (.has seen id))
    {:db/id id}
    (do
      (.add seen id)
      (-> (transient {})
        (pull-attrs db pull-pattern id seen)
        (pull-reverse-attrs db pull-pattern id seen)
        (persistent!)))))

(defn pull [db pattern id]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)]
    (pull-impl db pull-pattern id #?(:clj (java.util.HashSet.) :cljs (js/Set.)))))

(defn pull-many [db pattern ids]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)
        seen         #?(:clj (java.util.HashSet.) :cljs (js/Set.))]
    (mapv #(pull-impl db pull-pattern % seen) ids)))

(defn test-pull-v2 []
  (binding [clojure.test/*report-counters* (ref clojure.test/*initial-report-counters*)]
    (clojure.test/test-vars
      [#'datascript.test.pull-parser-v2/test-parse-pattern 
       #'datascript.test.pull-api/test-pull-attr-spec
       #'datascript.test.pull-api/test-pull-reverse-attr-spec
       #'datascript.test.pull-api/test-pull-component-attr
       #'datascript.test.pull-api/test-pull-wildcard
       #'datascript.test.pull-api/test-pull-limit
       ; #'datascript.test.pull-api/test-pull-default
       #'datascript.test.pull-api/test-pull-as
       ; #'datascript.test.pull-api/test-pull-attr-with-opts
       ; #'datascript.test.pull-api/test-pull-map
       ; #'datascript.test.pull-api/test-pull-recursion
       ; #'datascript.test.pull-api/test-dual-recursion
       ; #'datascript.test.pull-api/test-deep-recursion
       ; #'datascript.test.pull-api/test-lookup-ref-pull
       ])
    @clojure.test/*report-counters*))
