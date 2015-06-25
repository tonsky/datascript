(ns datascript.shim
  (:require
    [clojure.string :as str])
  (:refer-clojure :exclude [make-array into-array array amap aget aset alength array? seqable? ])
  #?(:cljs (:require-macros datascript.shim))
  #?(:clj  (:import [java.util Arrays])))

#?(:cljs (def make-array cljs.core/make-array)
   :clj  (defn make-array ^{:tag "[[Ljava.lang.Object;"}
           [size]
           (clojure.core/make-array java.lang.Object size)))

#?(:cljs (def into-array cljs.core/into-array)
   :clj  (defn into-array ^{:tag "[[Ljava.lang.Object;"}
           [aseq]
           (clojure.core/into-array java.lang.Object aseq)))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:clj
  (defmacro array [& args]
    (if-cljs &env
      (->
        (list* 'js* (str "[" (str/join "," (repeat (count args) "~{}")) "]") args)
        (vary-meta assoc :tag 'array))
     `(clojure.core/into-array Object ~(vec args)))))

#?(:clj
  (defmacro aget [arr i]
    (if-cljs &env
     `(cljs.core/aget ~arr ~i)
     `(clojure.lang.RT/aget ~(vary-meta arr assoc :tag "[[Ljava.lang.Object;") (int ~i)))))

#?(:clj
  (defmacro alength [arr]
    (if-cljs &env
     `(cljs.core/alength ~arr)
     `(clojure.lang.RT/alength ~(vary-meta arr assoc :tag "[[Ljava.lang.Object;")))))

#?(:clj
  (defmacro aset [arr i v]
    (if-cljs &env
     `(cljs.core/aset ~arr ~i ~v)
     `(clojure.lang.RT/aset ~(vary-meta arr assoc :tag "[[Ljava.lang.Object;") (int ~i) ~v))))

(defn aconcat [a b]
  #?(:cljs (.concat a b)
     :clj  (let [al  (alength a)
                 bl  (alength b)
                 res (Arrays/copyOf ^{:tag "[[Ljava.lang.Object;"} a (+ al bl))]
             (System/arraycopy ^{:tag "[[Ljava.lang.Object;"} b 0 res al bl)
             res)))

(defn amap [f arr]
  #?(:cljs (.map arr f)
     :clj  (clojure.core/amap ^{:tag "[[Ljava.lang.Object;"} arr i res (f (aget arr i)))))

(defn asort [arr cmp]
  #?(:cljs (.sort arr cmp)
     :clj  (doto arr (java.util.Arrays/sort cmp))))

(def array?
  #?(:cljs cljs.core/array?
     :clj  (fn array? [^Object x] (-> x .getClass .isArray))))

(def seqable?
  #?(:cljs cljs.core/seqable?
     :clj (fn seqable? [x]
            (or (seq? x)
                (instance? clojure.lang.Seqable x)
                (nil? x)
                (instance? Iterable x)
                (array? x)
                (string? x)
                (instance? java.util.Map x)))))

(def neg-number? (every-pred number? neg?))
