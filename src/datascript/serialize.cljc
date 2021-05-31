(ns datascript.serialize
  (:refer-clojure :exclude [amap])
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [raise cond+] #?@(:cljs [:refer [Datom]])]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.arrays :as arrays])
  #?(:cljs (:require-macros [datascript.serialize :refer [array dict]]))
  #?(:clj
     (:import
      [datascript.db Datom])))

(def ^:const marker-kw 0)
(def ^:const marker-other 1)

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:clj
   (defmacro array
     "Platform-native array representation (java.util.List on JVM, Array on JS)"
     [& args]
     (if-cljs &env
       (list* 'js* (str "[" (str/join "," (repeat (count args) "~{}")) "]") args)
       `(java.util.List/of ~@args))))

#?(:clj
   (defmacro dict
     "Platform-native dictionary representation (java.util.Map on JVM, Object on JS)"
     [& args]
     (if-cljs &env
       (list* 'js* (str "{" (str/join "," (repeat (/ (count args) 2) "~{}:~{}")) "}") args)
       `(array-map ~@args))))

(defn- array-get [d i]
  #?(:clj  (.get ^java.util.List d (int i))
     :cljs (arrays/aget d i)))

(defn- dict-get [d k]
  #?(:clj  (.get ^java.util.Map d k)
     :cljs (arrays/aget d k)))

(defn- amap [f xs]
  #?(:clj
     (let [arr (java.util.ArrayList. (count xs))]
       (reduce (fn [idx x] (.add arr (f x)) (inc idx)) 0 xs)
       arr)
     :cljs
     (let [arr (js/Array. (count xs))]
       (reduce (fn [idx x] (arrays/aset arr idx (f x)) (inc idx)) 0 xs)
       arr)))

(defn- amap-indexed [f xs]
  #?(:clj
     (let [arr (java.util.ArrayList. (count xs))]
       (reduce (fn [idx x] (.add arr (f idx x)) (inc idx)) 0 xs)
       arr)
     :cljs
     (let [arr (js/Array. (count xs))]
       (reduce (fn [idx x] (arrays/aset arr idx (f idx x)) (inc idx)) 0 xs)
       arr)))

(defn- attr-comparator
  "Looks for a datom with attribute exactly bigger than the given one"
  [^Datom d1 ^Datom d2]
  (cond 
    (nil? (.-a d2)) -1
    (<= (compare (.-a d1) (.-a d2)) 0) -1
    true 1))

(defn- all-attrs
  "All attrs in a DB, distinct, sorted"
  [db]
  (if (empty? (:aevt db))
    []
    (loop [attrs (transient [(:a (first (:aevt db)))])]
      (let [attr      (nth attrs (dec (count attrs)))
            left      (db/datom 0 attr nil)
            right     (db/datom db/emax nil nil)
            next-attr (:a (first (set/slice (:aevt db) left right attr-comparator)))]
        (if (some? next-attr)
          (recur (conj! attrs next-attr))
          (persistent! attrs))))))

(def ^{:arglists '([kw])} freeze-kw str)

(defn thaw-kw [s]
  (if (str/starts-with? s ":")
    (keyword (subs s 1))
    s))

(defn ^:export serializable
  "Converts db into a data structure (not string!) that can be fed to JSON
   serializer of your choice (`js/JSON.stringify` in CLJS, `cheshire.core/generate-string` or
   `jsonista.core/write-value-as-string` in CLJ).

   Options:

   Non-primitive values will be serialized using optional :freeze-fn (`pr-str` by default).

   Serialized structure breakdown:

   count    :: number    
   tx0      :: number
   max-eid  :: number
   max-tx   :: number
   schema   :: freezed :schema
   attrs    :: [keywords ...]
   keywords :: [keywords ...]
   eavt     :: [[e a-idx v dtx] ...]
   a-idx    :: index in attrs
   v        :: (string | number | boolean | [0 <index in keywords>] | [1 <freezed v>])
   dtx      :: tx - tx0
   aevt     :: [<index in eavt> ...]
   avet     :: [<index in eavt> ...]"
  ([db]
   (serializable db {}))
  ([db {:keys [freeze-fn]
        :or   {freeze-fn pr-str}}]
   (let [attrs       (all-attrs db)
         attrs-map   (into {} (map vector attrs (range)))
         *kws        (volatile! (transient []))
         *kw-map     (volatile! (transient {}))
         write-kw    (fn [kw]
                       (let [idx (or
                                   (get @*kw-map kw)
                                   (let [keywords (vswap! *kws conj! kw)
                                         idx      (dec (count keywords))]
                                     (vswap! *kw-map assoc! kw idx)
                                     idx))]
                         (array marker-kw idx)))
         write-other (fn [v] (array marker-other (freeze-fn v)))
         write-v     (fn [v]
                       (cond
                         (string? v)  v
                         #?@(:clj [(ratio? v) (write-other v)])
                         (number? v)  v
                         (boolean? v) v
                         (keyword? v) (write-kw v)
                         :else        (write-other v)))
         eavt        (amap-indexed
                       (fn [idx ^Datom d]
                         (db/datom-set-idx d idx)
                         (let [e  (.-e d)
                               a  (attrs-map (.-a d))
                               v  (write-v (.-v d))
                               tx (- (.-tx d) db/tx0)]
                           (array e a v tx)))
                       (:eavt db))
         aevt        (amap-indexed (fn [_ ^Datom d] (db/datom-get-idx d)) (:aevt db))
         avet        (amap-indexed (fn [_ ^Datom d] (db/datom-get-idx d)) (:avet db))
         schema      (freeze-fn (:schema db))
         attrs       (amap freeze-kw attrs)
         kws         (amap freeze-kw (persistent! @*kws))]
       (dict
         "count"    (count (:eavt db))
         "tx0"      db/tx0
         "max-eid"  (:max-eid db)
         "max-tx"   (:max-tx db)
         "schema"   schema
         "attrs"    attrs
         "keywords" kws
         "eavt"     eavt
         "aevt"     aevt
         "avet"     avet))))

(defn ^:export from-serializable
  "Creates db from a data structure (not string!) produced by serializable.

   Non-primitive values will be deserialized using optional :thaw-fn
   (`clojure.edn/read-string` by default).

   :thaw-fn must match :freeze-fn from serializable."
  ([serializable] 
   (from-serializable serializable {}))
  ([serializable {:keys [thaw-fn]
                  :or   {thaw-fn edn/read-string}}]
   (let [tx0      (dict-get serializable "tx0")
         schema   (thaw-fn (dict-get serializable "schema"))
         _        (#'db/validate-schema schema)
         attrs    (->> (dict-get serializable "attrs") (mapv thaw-kw))
         keywords (->> (dict-get serializable "keywords") (mapv thaw-kw))
         eavt     (->> (dict-get serializable "eavt")
                    (amap (fn [arr]
                            (let [e  (array-get arr 0)
                                  a  (nth attrs (array-get arr 1))
                                  v  (array-get arr 2)
                                  v  (cond
                                       (number? v)  v
                                       (string? v)  v
                                       (boolean? v) v
                                       (arrays/array? v)
                                       (let [marker (array-get v 0)]
                                         (case marker
                                           marker-kw    (array-get keywords (array-get v 1))
                                           marker-other (thaw-fn (array-get v 1)))))
                                  tx (+ tx0 (array-get arr 3))]
                              (db/datom e a v tx))))
                    #?(:clj .toArray))
         aevt     (some->> (dict-get serializable "aevt") (amap #(arrays/aget eavt %)) #?(:clj .toArray))
         avet     (some->> (dict-get serializable "avet") (amap #(arrays/aget eavt %)) #?(:clj .toArray))]
     (db/map->DB
       {:schema  schema
        :rschema (#'db/rschema (merge db/implicit-schema schema))
        :eavt    (set/from-sorted-array db/cmp-datoms-eavt eavt)
        :aevt    (set/from-sorted-array db/cmp-datoms-aevt aevt)
        :avet    (set/from-sorted-array db/cmp-datoms-avet avet)
        :max-eid (dict-get serializable "max-eid")
        :max-tx  (dict-get serializable "max-tx")
        :hash    (atom 0)}))))
