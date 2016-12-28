(ns datascript.arrays
  (:require
    [clojure.string :as str])
  (:refer-clojure :exclude [make-array into-array array amap aget aset alength array? aclone])
  #?(:cljs (:require-macros datascript.arrays))
  #?(:clj  (:import [java.util Arrays])))

;; Clojure will cast to a `Long` if this is attempted in a `let` block.
#?(:clj (def ^:const zero-int (int 0)))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:cljs (def make-array cljs.core/make-array)
   :clj  (defn make-array ^{:tag "[[Ljava.lang.Object;"}
           [size]
           (clojure.core/make-array java.lang.Object size)))

#?(:cljs (def into-array cljs.core/into-array)
   :clj  (defn into-array ^{:tag "[[Ljava.lang.Object;"}
           [aseq]
           (clojure.core/into-array java.lang.Object aseq)))

#?(:clj
  (defmacro aget [arr i]
    (if-cljs &env
      (list 'js* "(~{}[~{}])" arr i)
     `(clojure.lang.RT/aget ~(vary-meta arr assoc :tag "[[Ljava.lang.Object;") (int ~i)))))

#?(:clj
  (defmacro alength [arr]
    (if-cljs &env
      (-> (list 'js* "~{}.length" arr)
          (vary-meta assoc :tag 'number))
     `(clojure.lang.RT/alength ~(vary-meta arr assoc :tag "[[Ljava.lang.Object;")))))

#?(:clj
  (defmacro aset [arr i v]
    (if-cljs &env
      (list 'js* "(~{}[~{}] = ~{})" arr i v)
     `(clojure.lang.RT/aset ~(vary-meta arr assoc :tag "[[Ljava.lang.Object;") (int ~i) ~v))))

#?(:clj
  (defmacro array [& args]
    (if-cljs &env
      (->
        (list* 'js* (str "[" (str/join "," (repeat (count args) "~{}")) "]") args)
        (vary-meta assoc :tag 'array))
      (let [len (count args)]
        (if (zero? len)
          'clojure.lang.RT/EMPTY_ARRAY
         `(let [arr# (clojure.core/make-array java.lang.Object ~len)]
            (doto ^{:tag "[[Ljava.lang.Object;"} arr#
            ~@(map #(list 'aset % (nth args %)) (range len)))))))))

#?(:clj
  (defmacro acopy [from from-start from-end to to-start]
    (if-cljs &env
     `(let [l# (- ~from-end ~from-start)]
        (dotimes [i# l#]
          (aset ~to (+ i# ~to-start) (aget ~from (+ i# ~from-start)))))
     `(let [l# (- ~from-end ~from-start)]
        (when (pos? l#)
          (System/arraycopy ^{:tag "[[Ljava.lang.Object;"} ~from
                            (int ~from-start)
                            ^{:tag "[[Ljava.lang.Object;"} ~to
                            (int ~to-start)
                            l#))))))

(defn aclone [from]
  #?(:clj  (Arrays/copyOf ^{:tag "[[Ljava.lang.Object;"} from (alength from))
     :cljs (.slice from 0)))


(defn aconcat [a b]
  #?(:cljs (.concat a b)
     :clj  (let [al (int (alength ^{:tag "[[Ljava.lang.Object;"} a))
                 bl (int (alength ^{:tag "[[Ljava.lang.Object;"} b))
                 res (Arrays/copyOf ^{:tag "[[Ljava.lang.Object;"} a
                                    (unchecked-add-int al bl))]
             (System/arraycopy ^{:tag "[[Ljava.lang.Object;"} b zero-int
                               res al bl)
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
