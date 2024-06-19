(ns datascript.built-ins
  (:require
    [clojure.string :as str]
    [datascript.db :as db]
    [datascript.impl.entity :as de]
    [datascript.util :as util]))

(defn- -differ? [& xs]
  (let [l (count xs)]
    (not= (take (/ l 2) xs) (drop (/ l 2) xs))))

(defn- -get-else
  [db e a else-val]
  (when (nil? else-val)
    (util/raise "get-else: nil default value is not supported" {:error :query/where}))
  (if-some [datom (first (db/-search db [(db/entid db e) a]))]
    (:v datom)
    else-val))

(defn- -get-some
  [db e & as]
  (reduce
    (fn [_ a]
      (when-some [datom (first (db/-search db [(db/entid db e) a]))]
        (reduced [(:a datom) (:v datom)])))
    nil
    as))

(defn- -missing?
  [db e a]
  (nil? (get (de/entity db e) a)))

(defn- and-fn [& args]
  (reduce (fn [a b]
            (if b b (reduced b))) true args))
            
(defn- or-fn [& args]
  (reduce (fn [a b]
            (if b (reduced b) b)) nil args))

(defn- less
  ([x] true)
  ([x y] (neg? (db/value-compare x y)))
  ([x y & more]
   (if (less x y)
     (if (next more)
       (recur y (first more) (next more))
       (less y (first more)))
     false)))

(defn- greater
  ([x] true)
  ([x y] (pos? (db/value-compare x y)))
  ([x y & more]
   (if (greater x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater y (first more)))
     false)))

(defn- less-equal
  ([x] true)
  ([x y] (not (pos? (db/value-compare x y))))
  ([x y & more]
   (if (less-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (less-equal y (first more)))
     false)))

(defn- greater-equal
  ([x] true)
  ([x y] (not (neg? (db/value-compare x y))))
  ([x y & more]
   (if (greater-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater-equal y (first more)))
     false)))

(def query-fns
  {'= =, '== ==, 'not= not=, '!= not=,
   '< less, '> greater, '<= less-equal, '>= greater-equal,
   '+ +, '- -, '* *, '/ /,
   'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec, 'max max, 'min min,
   'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'compare compare,
   'rand rand, 'rand-int rand-int,
   'true? true?, 'false? false?, 'nil? nil?, 'some? some?, 'not not, 'and and-fn, 'or or-fn,
   'complement complement, 'identical? identical?,
   'identity identity, 'keyword keyword, 'meta meta, 'name name, 'namespace namespace, 'type type,
   'vector vector, 'list list, 'set set, 'hash-map hash-map, 'array-map array-map,
   'count count, 'range range, 'not-empty not-empty, 'empty? empty?, 'contains? contains?,
   'str str, 'subs, subs, 'get get,
   'pr-str pr-str, 'print-str print-str, 'println-str println-str, 'prn-str prn-str,
   're-find re-find, 're-matches re-matches, 're-seq re-seq, 're-pattern re-pattern,
   '-differ? -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?, 'ground identity,
   'clojure.string/blank? str/blank?, 'clojure.string/includes? str/includes?,
   'clojure.string/starts-with? str/starts-with?, 'clojure.string/ends-with? str/ends-with?
   'tuple vector, 'untuple identity})

;; Aggregates

(defn- aggregate-sum [coll]
  (reduce + 0 coll))

(defn- aggregate-avg [coll]
  (/ (aggregate-sum coll) (count coll)))

(defn- aggregate-median [coll]
  (let [terms (sort coll)
        size (count coll)
        med (bit-shift-right size 1)]
    (cond-> (nth terms med)
      (even? size)
      (-> (+ (nth terms (dec med)))
        (/ 2)))))

(defn- aggregate-variance [coll]
  (let [mean (aggregate-avg coll)
        sum  (aggregate-sum
               (for [x coll
                     :let [delta (- x mean)]]
                 (* delta delta)))]
    (/ sum (count coll))))

(defn- aggregate-stddev [coll]
  (#?(:cljs js/Math.sqrt :clj Math/sqrt) (aggregate-variance coll)))

(defn- aggregate-min
  ([coll]
   (reduce
     (fn [acc x]
       (if (neg? (compare x acc))
         x acc))
     (first coll) (next coll)))
  ([n coll]
   (vec
     (reduce (fn [acc x]
               (cond
                 (< (count acc) n)
                 (sort compare (conj acc x))
                 (neg? (compare x (last acc)))
                 (sort compare (conj (butlast acc) x))
                 :else acc))
       [] coll))))

(defn- aggregate-max
  ([coll]
   (reduce
     (fn [acc x]
       (if (pos? (compare x acc))
         x acc))
     (first coll) (next coll)))
  ([n coll]
   (vec
     (reduce (fn [acc x]
               (cond
                 (< (count acc) n)
                 (sort compare (conj acc x))
                 (pos? (compare x (first acc)))
                 (sort compare (conj (next acc) x))
                 :else acc))
       [] coll))))

(defn- aggregate-rand
  ([coll] (rand-nth coll))
  ([n coll] (vec (repeatedly n #(rand-nth coll)))))

(defn- aggregate-sample [n coll]
  (vec (take n (shuffle coll))))

(defn- aggregate-count-distinct [coll]
  (count (distinct coll)))

(def aggregates
  {'sum      aggregate-sum
   'avg      aggregate-avg
   'median   aggregate-median
   'variance aggregate-variance
   'stddev   aggregate-stddev
   'distinct set
   'min      aggregate-min
   'max      aggregate-max
   'rand     aggregate-rand
   'sample   aggregate-sample
   'count    count
   'count-distinct aggregate-count-distinct})
