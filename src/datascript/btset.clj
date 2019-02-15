(ns ^{:no-doc true
      :author "Nikita Prokopov"}
  datascript.btset
  (:require
    [datascript.arrays :as da])
  (:import
    [java.util Comparator Arrays]
    [datascript SortedSet SortedSet$Leaf SortedSet$Node SortedSet$Edit Stitch]))


(defn btset-conj [set key cmp]
  (.cons ^SortedSet set key cmp))


(defn btset-disj [set key cmp]
  (.disjoin ^SortedSet set key cmp))


(defn slice
  "`(slice set key)` returns iterator that contains all elements equal to the key.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   `(slice set from nil)` returns iterator for all Xs where X >= from."
  ([set key]
    (.slice ^SortedSet set key key))
  ([set from to]
    (.slice ^SortedSet set from to))
  ([set from to cmp]
    (.slice ^SortedSet set from to ^Comparator cmp)))


(defn rslice
  "`(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   `(rslice set from nil)` returns backwards iterator for all Xs where X <= from."
  ([set from to]
    (.rslice ^SortedSet set from to))
  ([set from to cmp]
    (.rslice ^SortedSet set from to ^Comparator cmp)))


(defn- array-from-indexed [coll type from to]
  (cond
    (instance? clojure.lang.Indexed coll)
    (Stitch/indexedToArray type coll from to)

    (da/array? coll)
    (Arrays/copyOfRange coll from to (da/array-type type))))


(defn- split
  ([coll to type avg max]
   (persistent! (split (transient []) 0 coll to type avg max)))
  ([res from coll to type avg max]
   (let [len (- to from)]
     (cond
       (== 0 len)
       res

       (>= len (* 2 avg))
       (recur (conj! res (array-from-indexed coll type from (+ from avg))) (+ from avg) coll to type avg max)

       (<= len max)
       (conj! res (array-from-indexed coll type from to))

       :else
       (-> res
         (conj! (array-from-indexed coll type from (+ from (quot len 2))))
         (conj! (array-from-indexed coll type (+ from (quot len 2)) to)))))))


(defn from-sorted-array
  ([cmp keys]
   (from-sorted-array cmp keys (da/alength keys)))
  ([cmp keys len]
   (let [max    SortedSet/MAX_LEN
         avg    (quot (+ SortedSet/MIN_LEN max) 2)
         edit   (SortedSet$Edit. false)
         ->Leaf (fn [keys]
                  (SortedSet$Leaf. keys (count keys) edit))
         ->Node (fn [children]
                  (SortedSet$Node.
                    (da/amap #(.maxKey ^SortedSet$Leaf %) Object children)
                    children (count children) edit))]
     (loop [nodes (mapv ->Leaf (split keys len Object avg max))]
       (case (count nodes)
         0 (SortedSet. cmp)
         1 (SortedSet. {} cmp (first nodes) len edit)
         (recur (mapv ->Node (split nodes (count nodes) SortedSet$Leaf avg max))))))))


(defn ctor [^Comparator cmp keys]
  (let [arr (to-array keys)
        _   (da/asort arr cmp)
        len (Stitch/distinct cmp arr)]
    (from-sorted-array cmp arr len)))


(defn btset-by
  ([cmp] (SortedSet. ^Comparator cmp))
  ([cmp & keys] (ctor cmp keys)))


(defn btset
  ([] (SortedSet.))
  ([& keys] (ctor compare keys)))
