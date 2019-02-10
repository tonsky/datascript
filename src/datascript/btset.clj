(ns ^{:no-doc true
      :author "Nikita Prokopov"}
  datascript.btset
  (:refer-clojure :exclude [iter])
  (:import
    [datascript SortedSet]))

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
    (.slice ^SortedSet set from to cmp)))

(defn rslice
  "`(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   `(rslice set from nil)` returns backwards iterator for all Xs where X <= from."
  ([set from to]
    (.rslice ^SortedSet set from to))
  ([set from to cmp]
    (.rslice ^SortedSet set from to cmp)))

(defn btset-by
  ([cmp] (SortedSet. cmp))
  ([cmp & keys]
    (into (btset-by cmp) keys)))

(defn btset
  ([] (SortedSet.))
  ([& keys]
    (into (btset) keys)))
