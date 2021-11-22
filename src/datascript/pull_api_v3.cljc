(ns ^:no-doc datascript.pull-api-v3
  (:require
   [datascript.pull-parser-v2 :as dpp]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [cond+]]
   [me.tonsky.persistent-sorted-set :as set])
  #?(:clj
     (:import
      [clojure.lang ISeq]
      [datascript.db Datom DB]
      [datascript.pull_parser_v2 PullAttr PullPattern])))

(declare pull-impl attrs-frame ref-frame ->ReverseAttrsFrame)

(defn- first-seq [#?(:clj ^ISeq xs :cljs ^seq xs)]
  (when (some? xs)
    #?(:clj (.first xs) :cljs (-first xs))))

(defn- next-seq [#?(:clj ^ISeq xs :cljs ^seq xs)]
  (when (some? xs)
    #?(:clj (.next xs) :cljs (-next xs))))

(defn- new-set []
  #?(:clj (java.util.HashSet.)
     :cljs (js/Set.)))

(defn- contains-set? [#?(:clj ^java.util.Set set :cljs set) el]
  (#?(:clj .contains :cljs .has) set el))

(defn- conj-set [#?(:clj ^java.util.Set set :cljs set) el]
  (.add set el)
  set)

(defprotocol IFrame
  (-merge [this result])
  (-run [this db seen]))

(defrecord ResultFrame [value datoms])

(defrecord MultivalAttrFrame [acc ^PullAttr attr datoms]
  IFrame
  (-run [this db seen]
    (loop [acc acc
           datoms datoms]
      (cond+
        :let [^Datom datom (first-seq datoms)]

        (or (nil? datom) (not= (.-a datom) (.-name attr)))
        [(ResultFrame. (persistent! acc) (or datoms ()))]

        (and (.-limit attr) (>= (count acc) (.-limit attr)))
        (loop [datoms datoms]
          (let [^Datom datom (first-seq datoms)]
            (if (or (nil? datom) (not= (.-a datom) (.-name attr)))
              [(ResultFrame. (persistent! acc) (or datoms ()))]
              (recur (next-seq datoms)))))

        :else
        (recur (conj! acc (.-v datom)) (next-seq datoms))))))

(defrecord MultivalRefAttrFrame [acc pattern ^PullAttr attr datoms]
  IFrame
  (-merge [this result]
    (MultivalRefAttrFrame. (conj! acc (.-value ^ResultFrame result)) pattern attr (next-seq datoms)))
  (-run [this db seen]
    (cond+
      :let [^Datom datom (first-seq datoms)]

      (or (nil? datom) (not= (.-a datom) (.-name attr)))
      [(ResultFrame. (persistent! acc) (or datoms ()))]

      (and (.-limit attr) (>= (count acc) (.-limit attr)))
      (loop [datoms datoms]
        (let [^Datom datom (first-seq datoms)]
          (if (or (nil? datom) (not= (.-a datom) (.-name attr)))
            [(ResultFrame. (persistent! acc) (or datoms ()))]
            (recur (next-seq datoms)))))

      :let [child-pattern (if (.-recursive? attr) pattern (.-pattern attr))
            id            (if (.-reverse? attr) (.-e datom) (.-v datom))]

      :else
      [this (attrs-frame db seen child-pattern id)])))

(defrecord AttrsFrame [acc ^PullPattern pattern ^PullAttr attr attrs datoms id]
  IFrame
  (-merge [this result]
    (AttrsFrame.
      (assoc! acc (.-as attr) (.-value ^ResultFrame result))
      pattern
      (first-seq attrs)
      (next-seq attrs)
      (not-empty (or (.-datoms ^ResultFrame result) (next-seq datoms)))
      id))
  (-run [this db seen]
    (loop [acc    acc
           attr   attr
           attrs  attrs
           datoms datoms]
      (cond+
        ;; exit
        (and (nil? datoms) (nil? attr))
        [(->ReverseAttrsFrame acc pattern (first-seq (.-reverse-attrs pattern)) (next-seq (.-reverse-attrs pattern)) id)]

        ;; :db/id
        (and (some? attr) (= :db/id (.-name attr)))
        (recur (assoc! acc (.-as attr) id) (first-seq attrs) (next-seq attrs) datoms)

        :let [^Datom datom (first-seq datoms)
              cmp (when (and datom attr)
                    (compare (.-name attr) (.-a datom)))]

        ;; wildcard
        (and (.-wildcard? pattern) (some? datom) (or (nil? attr) (neg? cmp)))
        (recur acc (dpp/parse-attr-name db (.-a datom)) attrs datoms)

        ;; advance attr
        (or (nil? datoms) (and (some? cmp) (neg? cmp)))
        (recur acc (first-seq attrs) (next-seq attrs) datoms)

        ;; advance datom
        (or (nil? attr) (and (some? cmp) (pos? cmp)))
        (recur acc attr attrs (next-seq datoms))

        ;; matching attr
        (and (.-multival? attr) (.-ref? attr))
        [(AttrsFrame. acc pattern attr attrs datoms id)
         (MultivalRefAttrFrame. (transient []) pattern attr datoms)]

        (.-multival? attr)
        [(AttrsFrame. acc pattern attr attrs datoms id)
         (MultivalAttrFrame. (transient []) attr datoms)]

        (.-ref? attr)
        [(AttrsFrame. acc pattern attr attrs datoms id)
         (ref-frame db seen pattern attr (.-v datom))]

        :else
        (recur (assoc! acc (.-as attr) (.-v datom)) attr attrs (next-seq datoms))))))

(defrecord ReverseAttrsFrame [acc pattern ^PullAttr attr attrs id]
  IFrame
  (-merge [this result]
    (ReverseAttrsFrame.
      (assoc! acc (.-as attr) (.-value ^ResultFrame result))
      pattern
      (first-seq attrs)
      (next-seq attrs)
      id))
  (-run [this db seen]
    (cond+
      (nil? attr)
      [(ResultFrame. (persistent! acc) nil)]

      :let [name   (.-name attr)
            datoms (set/slice (.-avet ^DB db) (db/datom db/e0 name id db/tx0) (db/datom db/emax name id db/txmax))]

      (.-component? attr)
      [this (ref-frame db seen pattern attr (.-e ^Datom (first-seq datoms)))]

      :else
      [this (MultivalRefAttrFrame. (transient []) pattern attr datoms)])))

(defn ref-frame [db seen pattern ^PullAttr attr id]
  (attrs-frame db seen (if (.-recursive? attr) pattern (.-pattern attr)) id))

(defn attrs-frame [^DB db seen ^PullPattern pattern id]
  (if (contains-set? seen id)
    (ResultFrame. {:db/id id} nil)
    (do
      (conj-set seen id)
      (AttrsFrame.
        (transient {})
        pattern
        (first-seq (.-attrs pattern))
        (next-seq (.-attrs pattern))
        (set/slice (.-eavt db) (db/datom id nil nil db/tx0) (db/datom id nil nil db/txmax))
        id))))

(defn- pull-impl [db seen ^PullPattern pattern id]
  (loop [stack (list (attrs-frame db seen pattern id))]
    (cond+
      :let [[last & stack'] stack]

      (not (instance? ResultFrame last))
      (recur (into stack' (-run last db seen)))

      (nil? stack')
      (.-value ^ResultFrame last)

      :let [[penultimate & stack''] stack']

      :else
      (recur (conj stack'' (-merge penultimate last))))))

(defn pull [db pattern id]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)]
    (pull-impl db (new-set) pull-pattern (db/entid db id))))

(defn pull-many [db pattern ids]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)
        seen         (new-set)]
    (mapv #(pull-impl db seen pull-pattern (db/entid db %)) ids)))

(comment
  (do
    (set! *warn-on-reflection* true)
    (require 'datascript.pull-api-v3 :reload-all))

  (require '[datascript.core :as d])
  (let [schema {:alias  {:db/cardinality :db.cardinality/many}
                :friend {:db/cardinality :db.cardinality/many
                         :db/valueType :db.type/ref}}
        db (d/db-with
             (d/empty-db schema)
             [{:db/id 1, :first-name "Ivan", :last-name "Ivanov",   :alias ["Vanya" "Ivanesku" "Ivan IV"], :age 32, :sex :male}
              {:db/id 2, :first-name "Oleg", :last-name "Petrov",   :alias ["Olezhka" "Olegan" "Ole-ole"], :age 55, :sex :male, :friend [1 3]}
              {:db/id 3, :first-name "Olga", :last-name "Sidorova", :alias ["Olya" "Galya" "Olega"], :age 15, :sex :female}])]
    (pull db [:first-name :unknown :age :alias :sex '* {:friend '...} {:_friend '...}] 1)))

(comment
  (do
    (set! *warn-on-reflection* true)
    (require 'datascript.test :reload-all)
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
      @clojure.test/*report-counters*)))