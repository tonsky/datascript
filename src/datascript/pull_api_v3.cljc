(ns ^:no-doc datascript.pull-api-v3
  (:require
   [datascript.pull-parser-v2 :as dpp]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [cond+]]
   [datascript.lru :as lru]
   [me.tonsky.persistent-sorted-set :as set])
  #?(:clj
     (:import
      [clojure.lang ISeq]
      [datascript.db Datom DB]
      [datascript.pull_parser_v2 PullAttr PullPattern])))

(def ^:dynamic *pattern-cache* (lru/cache 100))

(declare pull-impl attrs-frame ref-frame ->ReverseAttrsFrame)

(defn- first-seq [#?(:clj ^ISeq xs :cljs ^seq xs)]
  (if (nil? xs)
    nil
    #?(:clj (.first xs) :cljs (-first xs))))

(defn- next-seq [#?(:clj ^ISeq xs :cljs ^seq xs)]
  (if (nil? xs)
    nil
    #?(:clj (.next xs) :cljs (-next xs))))

(defn- conj-seq [#?(:clj ^ISeq xs :cljs ^seq xs) x]
  (if (nil? xs)
    (list x)
    #?(:clj (.cons xs x) :cljs (-conj xs x))))

(defn- assoc-some! [m k v]
  (if (nil? v) m (assoc! m k v)))

(defn- conj-some! [xs v]
  (if (nil? v) xs (conj! xs v)))

(defrecord Context [db *attrs-cache visitor])

(defn visit [^Context context pattern e a v]
  (when-some [visitor (.-visitor context)]
    (visitor pattern e a v)))

(defprotocol IFrame
  (-merge [this result])
  (-run [this context]))

(defrecord ResultFrame [value datoms])

(defrecord MultivalAttrFrame [acc ^PullAttr attr datoms]
  IFrame
  (-run [this context]
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
  (-run [this context]
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
      [this (ref-frame context seen recursion-limits pattern attr id)])))

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
  (-run [this context]
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
              cmp          (when (and datom attr)
                             (compare (.-name attr) (.-a datom)))
              attr-ahead?  (or (nil? attr) (and cmp (pos? cmp)))
              datom-ahead? (or (nil? datom) (and cmp (neg? cmp)))]


        ; wildcard
        (and (.-wildcard? pattern) (some? datom) attr-ahead?)
        (let [*attrs-cache (.-*attrs-cache ^Context context)
              datom-attr   (or (@*attrs-cache (.-a datom))
                             (let [datom-attr (dpp/parse-attr-name (.-db ^Context context) (.-a datom))]
                               (vswap! *attrs-cache assoc! (.-a datom) datom-attr)
                               datom-attr))]
          (recur acc datom-attr (when attr (conj-seq attrs attr)) datoms))

        ;; advance datom
        attr-ahead?
        (recur acc attr attrs (next-seq datoms))

        :do (visit context :db.pull/attr id (.-name attr) nil)

        ; default
        (and attr (some? (.-default attr)) datom-ahead?)
        (recur (assoc! acc (.-as attr) (.-default attr)) (first-seq attrs) (next-seq attrs) datoms)

        ;; advance attr
        datom-ahead?
        (recur acc (first-seq attrs) (next-seq attrs) datoms)

        ;; matching attr
        (and (.-multival? attr) (.-ref? attr))
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalRefAttrFrame. seen recursion-limits (transient []) pattern attr datoms)]

        (.-multival? attr)
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalAttrFrame. (transient []) attr datoms)]

        (.-ref? attr)
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (ref-frame context seen recursion-limits pattern attr (.-v datom))]

        :else
        (recur (assoc! acc (.-as attr) (.-v datom)) (first-seq attrs) (next-seq attrs) (next-seq datoms))))))

(defrecord ReverseAttrsFrame [seen recursion-limits acc pattern ^PullAttr attr attrs id]
  IFrame
  (-merge [this result]
    (ReverseAttrsFrame.
      seen
      recursion-limits
      (assoc-some! acc (.-as attr) (.-value ^ResultFrame result))
      pattern
      (first-seq attrs)
      (next-seq attrs)
      id))
  (-run [this context]
    (cond+
      (nil? attr)
      [(ResultFrame. (not-empty (persistent! acc)) nil)]

      :let [name   (.-name attr)
            datoms (set/slice (.-avet ^DB (.-db ^Context context)) (db/datom db/e0 name id db/tx0) (db/datom db/emax name id db/txmax))]

      :do (visit context :db.pull/reverse nil name id)

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
      [this (ref-frame context seen recursion-limits pattern attr (.-e ^Datom (first-seq datoms)))]

      :else
      [this (MultivalRefAttrFrame. seen recursion-limits (transient []) pattern attr datoms)])))

(defn ref-frame [context seen recursion-limits pattern ^PullAttr attr id]
  (cond+
    (and (not (.-recursive? attr)) (not (.-component? attr)))
    (attrs-frame context seen recursion-limits (.-pattern attr) id)

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
    (attrs-frame context seen' recursion-limits' (if (.-recursive? attr) pattern (.-pattern attr)) id)))


(defn attrs-frame [^Context context seen recursion-limits ^PullPattern pattern id]
  (let [datoms (cond+
                 (.-wildcard? pattern)
                 (set/slice (.-eavt ^DB (.-db context))
                   (db/datom id nil nil db/tx0)
                   (db/datom id nil nil db/txmax))

                 (nil? (.-first-attr pattern))
                 nil

                 :else
                 (set/slice (.-eavt ^DB (.-db context))
                   (db/datom id (.-name ^PullAttr (.-first-attr pattern)) nil db/tx0)
                   (db/datom id (.-name ^PullAttr (.-last-attr pattern)) nil db/txmax)))]
    (when (.-wildcard? pattern)
      (visit context :db.pull/wildcard id nil nil))
    (AttrsFrame.
      seen
      recursion-limits
      (transient {})
      pattern
      (first-seq (.-attrs pattern))
      (next-seq (.-attrs pattern))
      datoms
      id)))

(defn- pull-impl [^Context context ^PullPattern pattern id]
  (loop [stack (list (attrs-frame context #{} {} pattern id))]
    (cond+
      :let [last   (first-seq stack)
            stack' (next-seq stack)]

      (not (instance? ResultFrame last))
      (recur (reduce conj-seq stack' (-run last context)))

      (nil? stack')
      (.-value ^ResultFrame last)

      :let [penultimate (first-seq stack')
            stack''     (next-seq stack')]

      :else
      (recur (conj-seq stack'' (-merge penultimate last))))))

(defn pull
  "Supported opts:

   :visitor a fn of 4 arguments, will be called for every entity/attribute pull touches

   (:db.pull/attr     e   a   nil) - when pulling a normal attribute, no matter if it has value or not
   (:db.pull/wildcard e   nil nil) - when pulling every attribute on an entity
   (:db.pull/reverse  nil a   v  ) - when pulling reverse attribute"
  ([^DB db pattern id] (pull db pattern id {}))
  ([^DB db pattern id {:keys [visitor]}]
   {:pre [(db/db? db)]}
     (when-some [eid (db/entid db id)]
       (let [pull-pattern (lru/-cache-get *pattern-cache* [(.-rschema db) pattern] #(dpp/parse-pattern db pattern))
             context      (Context. db (volatile! (transient {})) visitor)]
         (pull-impl context pull-pattern eid)))))

(defn pull-many
  ([^DB db pattern ids] (pull-many db pattern ids {}))
  ([^DB db pattern ids {:keys [visitor]}]
   {:pre [(db/db? db)]}
   (let [pull-pattern (lru/-cache-get *pattern-cache* [(.-rschema db) pattern] #(dpp/parse-pattern db pattern))
         context      (Context. db (volatile! (transient {})) visitor)]
     (mapv #(some->> % (db/entid db) (pull-impl context pull-pattern)) ids))))

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