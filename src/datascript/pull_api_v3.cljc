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

(defn- assoc-some! [m k v]
  (if (some? v) (assoc! m k v) m))

(defn- conj-some! [xs v]
  (if (some? v) (conj! xs v) xs))

(defprotocol IFrame
  (-merge [this result])
  (-run [this db attrs-cache]))

(defrecord ResultFrame [value datoms])

(defrecord MultivalAttrFrame [acc ^PullAttr attr datoms]
  IFrame
  (-run [this db attrs-cache]
    (loop [acc acc
           datoms datoms]
      (cond+
        :let [^Datom datom (first-seq datoms)]

        (or (nil? datom) (not= (.-a datom) (.-name attr)))
        [(ResultFrame. (not-empty (persistent! acc)) (or datoms ()))]

        ; got limit, skip rest of the datoms
        (and (.-limit attr) (>= (count acc) (.-limit attr)))
        (loop [datoms datoms]
          (let [^Datom datom (first-seq datoms)]
            (if (or (nil? datom) (not= (.-a datom) (.-name attr)))
              [(ResultFrame. (persistent! acc) (or datoms ()))]
              (recur (next-seq datoms)))))

        :else
        (recur (conj! acc (.-v datom)) (next-seq datoms))))))

(defrecord MultivalRefAttrFrame [seen recursion-limits acc pattern ^PullAttr attr datoms]
  IFrame
  (-merge [this result]
    (MultivalRefAttrFrame.
      seen
      recursion-limits
      (conj-some! acc (.-value ^ResultFrame result))
      pattern
      attr
      (next-seq datoms)))
  (-run [this db attrs-cache]
    (cond+
      :let [^Datom datom (first-seq datoms)]

      (or (nil? datom) (not= (.-a datom) (.-name attr)))
      [(ResultFrame. (not-empty (persistent! acc)) (or datoms ()))]

      ; got limit, skip rest of the datoms
      (and (.-limit attr) (>= (count acc) (.-limit attr)))
      (loop [datoms datoms]
        (let [^Datom datom (first-seq datoms)]
          (if (or (nil? datom) (not= (.-a datom) (.-name attr)))
            [(ResultFrame. (persistent! acc) (or datoms ()))]
            (recur (next-seq datoms)))))

      :let [id (if (.-reverse? attr) (.-e datom) (.-v datom))]

      :else
      [this (ref-frame db seen recursion-limits pattern attr id)])))

(defrecord AttrsFrame [seen recursion-limits acc ^PullPattern pattern ^PullAttr attr attrs datoms id]
  IFrame
  (-merge [this result]
    (AttrsFrame.
      seen
      recursion-limits
      (assoc-some! acc (.-as attr) (.-value ^ResultFrame result))
      pattern
      (first-seq attrs)
      (next-seq attrs)
      (not-empty (or (.-datoms ^ResultFrame result) (next-seq datoms)))
      id))
  (-run [this db attrs-cache]
    (loop [acc    acc
           attr   attr
           attrs  attrs
           datoms datoms]
      (cond+
        ;; exit
        (and (nil? datoms) (nil? attr))
        [(->ReverseAttrsFrame seen recursion-limits acc pattern (first-seq (.-reverse-attrs pattern)) (next-seq (.-reverse-attrs pattern)) id)]

        ;; :db/id
        (and (some? attr) (= :db/id (.-name attr)))
        (recur (assoc! acc (.-as attr) id) (first-seq attrs) (next-seq attrs) datoms)

        :let [^Datom datom (first-seq datoms)
              cmp (when (and datom attr)
                    (compare (.-name attr) (.-a datom)))
              attr-ahead?  (or (nil? attr) (and cmp (pos? cmp)))
              datom-ahead? (or (nil? datom) (and cmp (neg? cmp)))]


        ; wildcard
        (and (.-wildcard? pattern) (some? datom) attr-ahead?)
        (let [datom-attr (or (@attrs-cache (.-a datom))
                           (let [datom-attr (dpp/parse-attr-name db (.-a datom))]
                             (vswap! attrs-cache assoc! (.-a datom) datom-attr)
                             datom-attr))]
          (recur acc datom-attr (when attr (cons attr attrs)) datoms))

        ; default
        (and attr (some? (.-default attr)) datom-ahead?)
        (recur (assoc! acc (.-as attr) (.-default attr)) (first-seq attrs) (next-seq attrs) datoms)

        ;; advance attr
        datom-ahead?
        (recur acc (first-seq attrs) (next-seq attrs) datoms)

        ;; advance datom
        attr-ahead?
        (recur acc attr attrs (next-seq datoms))

        ;; matching attr
        (and (.-multival? attr) (.-ref? attr))
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalRefAttrFrame. seen recursion-limits (transient []) pattern attr datoms)]

        (.-multival? attr)
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalAttrFrame. (transient []) attr datoms)]

        (.-ref? attr)
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (ref-frame db seen recursion-limits pattern attr (.-v datom))]

        :else
        (recur (assoc! acc (.-as attr) (.-v datom)) (first-seq attrs) (next-seq attrs) (next-seq datoms))))))

(defrecord ReverseAttrsFrame [seen recursion-limits acc pattern ^PullAttr attr attrs id]
  IFrame
  (-merge [this result]
    (ReverseAttrsFrame.
      seen
      recursion-limits
      (if (some? (.-value ^ResultFrame result))
        (assoc! acc (.-as attr) (.-value ^ResultFrame result))
        acc)
      pattern
      (first-seq attrs)
      (next-seq attrs)
      id))
  (-run [this db attrs-cache]
    (cond+
      (nil? attr)
      [(ResultFrame. (not-empty (persistent! acc)) nil)]

      :let [name   (.-name attr)
            datoms (set/slice (.-avet ^DB db) (db/datom db/e0 name id db/tx0) (db/datom db/emax name id db/txmax))]

      (and (empty? datoms) (some? (.-default attr)))
      [(ReverseAttrsFrame.
          seen
          recursion-limits
          (assoc! acc (.-as attr) (.-default attr))
          pattern
          (first-seq attrs)
          (next-seq attrs)
          id)]

      (.-component? attr)
      [this (ref-frame db seen recursion-limits pattern attr (.-e ^Datom (first-seq datoms)))]

      :else
      [this (MultivalRefAttrFrame. seen recursion-limits (transient []) pattern attr datoms)])))

(defn ref-frame [db seen recursion-limits pattern ^PullAttr attr id]
  (cond+
    (and (not (.-recursive? attr)) (not (.-component? attr)))
    (attrs-frame db seen recursion-limits (.-pattern attr) id)

    (seen id)
    (ResultFrame. {:db/id id} nil)

    :let [lim (recursion-limits attr)]

    (and lim (<= lim 0))
    (ResultFrame. nil nil)

    :let [seen' (conj seen id)
          recursion-limits' (cond
                              lim                      (update recursion-limits attr dec)
                              (.-recursion-limit attr) (assoc recursion-limits attr (dec (.-recursion-limit attr)))
                              :else                    recursion-limits)]

    :else
    (attrs-frame db seen' recursion-limits' (if (.-recursive? attr) pattern (.-pattern attr)) id)))

(defn attrs-frame [^DB db seen recursion-limits ^PullPattern pattern id]
  (AttrsFrame.
    seen
    recursion-limits
    (transient {})
    pattern
    (first-seq (.-attrs pattern))
    (next-seq (.-attrs pattern))
    (set/slice (.-eavt db) (db/datom id nil nil db/tx0) (db/datom id nil nil db/txmax))
    id))

(defn- pull-impl [db attrs-cache ^PullPattern pattern id]
  (loop [stack (list (attrs-frame db #{} {} pattern id))]
    (cond+
      :let [[last & stack'] stack]

      (not (instance? ResultFrame last))
      (recur (into stack' (-run last db attrs-cache)))

      (nil? stack')
      (.-value ^ResultFrame last)

      :let [[penultimate & stack''] stack']

      :else
      (recur (conj stack'' (-merge penultimate last))))))

(defn pull [db pattern id]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)
        eid          (db/entid db id)
        attrs-cache  (volatile! (transient {}))]
    (when eid
      (pull-impl db attrs-cache pull-pattern eid))))

(defn pull-many [db pattern ids]
  {:pre [(db/db? db)]}
  (let [pull-pattern (dpp/parse-pattern db pattern)
        attrs-cache  (volatile! (transient {}))]
    (mapv #(some->> % (db/entid db) (pull-impl db attrs-cache pull-pattern)) ids)))

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
         #'datascript.test.pull-api/test-pull-default
         #'datascript.test.pull-api/test-pull-as
         #'datascript.test.pull-api/test-pull-attr-with-opts
         #'datascript.test.pull-api/test-pull-map
         #'datascript.test.pull-api/test-pull-recursion
         #'datascript.test.pull-api/test-dual-recursion
         #'datascript.test.pull-api/test-deep-recursion
         #'datascript.test.pull-api/test-lookup-ref-pull
         ])
      @clojure.test/*report-counters*)))