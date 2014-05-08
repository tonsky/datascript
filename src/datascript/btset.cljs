(ns datascript.btset
  (:require [test.datascript.perf :as perf]))

(declare BTSet Node LeafNode)

(def ^:const min-len nil) ;; FIXME
(def ^:const max-len 128)
(def ^:const path-bit-offset 8)
(def ^:const path-mask max-len)
(def ^:dynamic *cmp* compare)

(defn eq [a b]  (== 0 (*cmp* a b)))
(defn half [x]  (fix (/ x 2)))

(defn binary-search ^number [^array arr ^number l ^number r k]
  (if (<= l r)
    (let [m   (half (+ l r))
          mk  (aget arr m)
          cmp ^number (*cmp* mk k)]
      (cond
        (neg? cmp) (recur arr (inc m) r k)
        (pos? cmp) (recur arr l (dec m) k)
        :else m))
    l))

(defn lookup ^number [^array arr key]
  (binary-search arr 0 (dec (alength arr)) key))

(defn -seek ^number [set key]
  (loop [node (.-root set)
         path  0
         path-offset 0
         depth 1]
    (let [keys (.-keys node)
          idx  (lookup keys key)]
      (if (== depth (.-depth set))
        (bit-or (bit-shift-left idx path-offset) path)
        (let [idx (if (== idx (alength keys)) (dec idx) idx)]
          (recur (aget (.-pointers node) idx)
                 (bit-or (bit-shift-left idx path-bit-offset) path)
                 (+ path-offset path-bit-offset)
                 (inc depth)))))))

(defn seek [set key]
  (binding [*cmp* (.-comparator set)]
    (-seek set key)))

;; Array operations

(defn cut-n-splice ^array [^array  arr
                           ^number cut-from
                           ^number cut-to
                           ^number splice-from
                           ^number splice-to
                           ^array  xs]
  (let [arr-l   (alength arr)
        xs-l    (alength xs)
        l1      (- splice-from cut-from)
        l2      (- cut-to splice-to)
        l1xs    (+ l1 xs-l)
        new-arr (make-array (+ l1 xs-l l2))]
    (dotimes [i l1]
      (aset new-arr i (aget arr (+ cut-from i))))
    (dotimes [i xs-l]
      (aset new-arr (+ i l1) (aget xs i)))
    (dotimes [i l2]
      (aset new-arr (+ i l1xs) (aget arr (+ splice-to i))))
    new-arr))

(defn cut
  (^array [^array arr ^number cut-from]
    (.slice arr cut-from))
  (^array [^array arr ^number cut-from ^number cut-to]
    (.slice arr cut-from cut-to)))

(defn splice ^array [^array  arr
                     ^number splice-from
                     ^number splice-to
                     ^array  xs]
  (cut-n-splice arr 0 (alength arr) splice-from splice-to xs))

(defn insert ^array [^array arr
                     ^number idx
                     ^array xs]
  (cut-n-splice arr 0 (alength arr) idx idx xs))

;; 

(defn replace-keys ^array [^array old-keys ^number idx ^array new-keys]
  (let [new-keys-l (alength new-keys)]    
    (if (and (== 1 new-keys-l)
             (eq (aget new-keys 0) (nth old-keys idx)))
      ;; new key matches old one, can reuse old keys array
      old-keys
      ;; else regular replace
      (splice old-keys idx (inc idx) new-keys))))

(defn lim-key [node]
  (aget (.-keys node) (dec (alength (.-keys node)))))

(deftype Node [keys pointers]
  Object
  (append [_ ^number idx nodes]
    (let [new-keys     (replace-keys keys     idx (.map nodes lim-key))
          new-pointers (splice   pointers idx (inc idx) nodes)]
      (if (<= (alength new-pointers) max-len)
        ;; ok as is
        #js [(Node. new-keys new-pointers)]
        ;; gotta split it up
        (let [middle  (half (alength new-pointers))]
          #js [(Node. (cut new-keys     0 middle)
                      (cut new-pointers 0 middle))
               (Node. (cut new-keys     middle)
                      (cut new-pointers middle))])))))
          
(deftype LeafNode [keys]
  Object
  (append [_ ^number idx key]
    (let [keys-l (alength keys)]
      (if (== keys-l max-len)
        ;; splitting
        (let [middle (half (inc keys-l))]
          (if (> idx middle)
            ;; new key goes to the second half
            #js [(LeafNode. (cut keys 0 middle))
                 (LeafNode. (cut-n-splice keys middle keys-l idx idx #js [key]))]
            ;; new key goes to the first half
            #js [(LeafNode. (cut-n-splice keys 0 middle idx idx #js [key]))
                 (LeafNode. (cut keys middle keys-l))]))
        ;; ok as is
        #js [(LeafNode. (splice keys idx idx #js [key]))]))))

(defn -btset-conj [node path key]
  (if (instance? LeafNode node)
    (.append node path key)
    (let [path-idx  (bit-and path path-mask)
          path-next (unsigned-bit-shift-right path path-bit-offset)]
      (.append node path-idx (-btset-conj (aget (.-pointers node) path-idx) path-next key)))))

(defn btset-conj [set key]
  (binding [*cmp* (.-comparator set)]
    (let [roots (-btset-conj (.-root set) (-seek set key) key)]
      (if (== (alength roots) 1)
        ;; keeping single root
        (BTSet. (aget roots 0) (.-depth set) *cmp*)
        ;; introducing new root
        (BTSet. (Node. (.map roots lim-key) roots) (inc (.-depth set)) *cmp*)))))

(deftype BTSet [root depth comparator]
  Object
  (toString [this]
    (pr-str (seq this)))
  
  ICollection
  (-conj [set key] (btset-conj set key))
  
  ILookup 
  (-lookup [set k]
    (-lookup set k nil))
  (-lookup [set k not-found]
    (seek set k)) ;; FIXME

;;   IPrintWithWriter
;;   (-pr-writer [o writer _]
;;     (dump (.-root o) writer ""))
  )

(defn btset
  ([] (btset compare))
  ([cmp]
    (BTSet. (LeafNode. (array)) 1 cmp)))

;; helpers

(defn parse-path [path]
  (loop [path path
         res  []]
    (if (not= path 0)
      (recur (unsigned-bit-shift-right path path-bit-offset)
             (conj res (bit-and path path-mask)))
      res)))

(def seek-h (comp parse-path seek))

(defn dump [node writer offset]
  (if (instance? LeafNode node)
    (do
      (-write writer offset)
      (-write writer (vec (.-keys node)))
      (-write writer "\n"))
    (dotimes [i (alength (.-keys node))]
      (-write writer offset)
      (-write writer (aget (.-keys node) i))
      (-write writer "\n")
      (dump (aget (.-pointers node) i) writer (str "  " offset)))))

;; perf

(def test-matrix [:target  { "sorted-set" (sorted-set)
                             "btset"      (btset)}
                  :size    [100 500 1000 2000 5000 10000 20000 50000]
                  ;; :size    [100 500]
                  :method  { "conj"   (fn [opts] (into (:target opts) (:range opts)))
                             "lookup" (fn [opts] (contains? (:set opts) (rand-int (:size opts)))) }])

(defn test-setup [opts]
  (let [opts (assoc opts
               :range (shuffle (range (:size opts))))]
    (case (:method opts)
      "seek" (assoc opts :set (into (:target opts) (:range opts)))
      opts)))

(defn ^:export perftest []
  (perf/suite (fn [opts] ((:method opts) opts))
    :duration 5000
    :matrix   test-matrix
    :setup-fn test-setup))

;; (perftest)
