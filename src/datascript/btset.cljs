(ns datascript.btset
  (:require [test.datascript.perf :as perf]))

(declare BTSet Node LeafNode)

(def ^:const min-len nil) ;; FIXME
(def ^:const max-len 128)
(def ^:const path-bit-offset 8)
(def ^:const path-mask (dec (bit-shift-left 1 path-bit-offset)))
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
                 (bit-or (bit-shift-left idx path-offset) path)
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
  (get_key [_ path]
    (let [path-idx (bit-and path path-mask)
          path-next (unsigned-bit-shift-right path path-bit-offset)]
      (.get_key (aget pointers path-idx) path-next)))
  
  (next_path [_ path]
    (let [path-idx      (bit-and path path-mask)
          path-next     (unsigned-bit-shift-right path path-bit-offset)
          path-next-new (.next_path (aget pointers path-idx) path-next)]
      (if (== -1 path-next-new)
        (if (< (inc path-idx) (alength pointers))
          (inc path-idx)  ;; keeping subpathes filled with 0
          -1)
        (bit-or path-idx 
                (bit-shift-left path-next-new path-bit-offset)))))
  
  (append [_ path key]
    (let [path-idx  (bit-and path path-mask)
          path-next (unsigned-bit-shift-right path path-bit-offset)
          nodes     (.append (aget pointers path-idx) path-next key)
          new-keys     (replace-keys keys     path-idx                (.map nodes lim-key))
          new-pointers (splice       pointers path-idx (inc path-idx) nodes)]
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
  (get_key [_ path]
    (when (< path (alength keys))
      (aget keys path)))
  
  (next_path [_ path]
    (if (>= (inc path) (alength keys))
      -1
      (inc path)))
      
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

(defn next-path [set path]
  (.next_path (.-root set) path))

(defn get-key [set path]
  (.get_key (.-root set) path))

(defn btset-conj [set key]
  (binding [*cmp* (.-comparator set)]
    (let [roots (.append (.-root set) (-seek set key) key)]
      (if (== (alength roots) 1)
        ;; keeping single root
        (BTSet. (aget roots 0) (.-depth set) *cmp*)
        ;; introducing new root
        (BTSet. (Node. (.map roots lim-key) roots) (inc (.-depth set)) *cmp*)))))

(deftype BTSetIter [set path]
  ISeqable
  (-seq [this]
    (when-not (== -1 path)
      this))
  
  ISeq
  (-first [_]
    (when-not (== -1 path)
      (get-key set path)))
  
  (-rest [_]
    (BTSetIter. set (next-path set path))))

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
    (let [found (get-key set (seek set k))]
      (if (eq found k) found not-found)))

  ISeqable
  (-seq [this]
    (when (pos? (alength (.-keys root)))
      (BTSetIter. this 0))))

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

(extend-type BTSet
  IPrintWithWriter
  (-pr-writer [o writer _]
    (dump (.-root o) writer "")))


(defn test-rand []
  (dotimes [i 100]
    (let [xs (range (rand-int 10000))
          xss (shuffle xs)
          s  (into (btset) xss)]
      (when-not (= (vec s) xs)
        (println xss))))
  (println "Checked"))

;; (test-rand)

;; perf

(def test-matrix [:target  { "sorted-set" (sorted-set)
                             "btset"      (btset)}
                  :size    [100 500 1000 2000 5000 10000 20000 50000]
;;                   :size    [100 500]
                  :method  { ;; "conj"    (fn [opts] (into (:target opts) (:range opts)))
                             ;; "lookup"  (fn [opts] (contains? (:set opts) (rand-int (:size opts))))
                             "iterate" (fn [opts] (doseq [x (:set opts)] (+ 1 x))) }])

(defn test-setup [opts]
  (let [range (shuffle (range (:size opts)))]
    (-> opts
        (assoc :range range)
        (assoc :set (into (:target opts) range)))))

(defn ^:export perftest []
  (perf/suite (fn [opts] ((:method opts) opts))
    :duration 1000
    :matrix   test-matrix
    :setup-fn test-setup))

;; (perftest)
