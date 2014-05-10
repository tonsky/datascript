(ns datascript.btset
  (:require [test.datascript.perf :as perf]))

(declare BTSet Node LeafNode)

(def ^:const min-len nil) ;; FIXME
(def ^:const ^number max-len 128)
(def ^:const ^number level-shift (->> (range 31 -1 -1)
                              (filter #(bit-test max-len %))
                              first
                              inc))
(def ^:const ^number path-mask (dec (bit-shift-left 1 level-shift)))
(def ^:const ^number empty-path 0)
(def ^:dynamic *cmp* compare)

(defn ^number path-get [^number path
                        ^number level]
  (bit-and path-mask
           (unsigned-bit-shift-right path level)))

(defn ^number path-set [^number path
                        ^number level
                        ^number idx]
  (bit-or path 
          (bit-shift-left idx level)))

(defn eq ^boolean [^number a
                   ^number b]
  (== 0 (*cmp* a b)))

(defn ^number half [^number x]
  (fix (/ x 2)))

(defn ^number binary-search [^array arr ^number l ^number r k]
  (if (<= l r)
    (let [m   (half (+ l r))
          mk  (aget arr m)
          cmp ^number (*cmp* mk k)]
      (cond
        (neg? cmp) (recur arr (inc m) r k)
        (pos? cmp) (recur arr l (dec m) k)
        :else m))
    l))
    
(defn ^number lookup-leaf [^array arr key]
  (binary-search arr 0 (- (alength arr) 1) key))

(defn ^number lookup-node [^array arr key]
  (binary-search arr 0 (- (alength arr) 2) key))

(defn ^number -seek [set key]
  (loop [node  (.-root set)
         path  empty-path
         level (.-shift set)]
    (if (== 0 level)
      (path-set path level (lookup-leaf (.-keys node) key))
      (let [idx (lookup-node (.-keys node) key)]
        (recur (aget (.-pointers node) idx)
               (path-set path level idx)
               (- level level-shift))))))

(defn ^number seek [set key]
  (binding [*cmp* (.-comparator set)]
    (-seek set key)))

;; Array operations

(defn ^array cut-n-splice [^array  arr
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

(defn ^array splice [^array  arr
                     ^number splice-from
                     ^number splice-to
                     ^array  xs]
  (cut-n-splice arr 0 (alength arr) splice-from splice-to xs))

(defn ^array insert [^array arr
                     ^number idx
                     ^array xs]
  (cut-n-splice arr 0 (alength arr) idx idx xs))

;; 

(defn ^array replace-keys [^array old-keys ^number idx ^array new-keys]
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
  
  (append [_ path level key]
    (let [idx          (path-get path level)
          nodes        (.append (aget pointers idx) path (- level level-shift) key)
          new-keys     (replace-keys keys     idx           (.map nodes lim-key))
          new-pointers (splice       pointers idx (inc idx) nodes)]
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

  (append [_ ^number path _ key]
    (let [idx    (path-get path 0)
          keys-l (alength keys)]
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

(defn ^array keys-for [set ^number path]
  (loop [level (.-shift set)
         node  (.-root set)]
    (if (pos? level)
      (recur (- level level-shift) (aget (.-pointers node) (path-get path level)))
      (.-keys node))))

(defn key-for [set ^number path]
  (let [keys (keys-for set path)
        idx  (path-get path 0)]
    (when (< idx (alength keys)) 
      (aget keys idx))))

(defn btset-conj [set key]
  (binding [*cmp* (.-comparator set)]
    (let [roots (.append (.-root set) (-seek set key) (.-shift set) key)]
      (if (== (alength roots) 1)
        ;; keeping single root
        (BTSet. (aget roots 0) (.-shift set) *cmp*)
        ;; introducing new root
        (BTSet. (Node. (.map roots lim-key) roots) (+ (.-shift set) level-shift) *cmp*)))))

;; iteration

(defn ^number -next-path [node
                          ^number path
                          ^number level]
  (let [idx (path-get path level)]
    (if (pos? level)
      ;; inner node
      (let [sub-path (-next-path (aget (.-pointers node) idx) path (- level level-shift))]
        (if (== -1 sub-path)
          ;; nested node overflow
          (if (< (inc idx) (alength (.-pointers node)))
            ;; advance current node idx, reset subsequent indexes
            (path-set empty-path level (inc idx))
            ;; current node overflow
            -1)
          ;; keep current idx
          (path-set sub-path level idx)))
      ;; leaf
      (if (< (inc idx) (alength (.-keys node)))
        ;; advance leaf idx
        (path-set empty-path 0 (inc idx))
        ;; leaf overflow
        -1))))

(defn next-path [set path]
  (-next-path (.-root set) path (.-shift set)))


(deftype BTSetIter [^BTSet  set
                    ^number path
                    ^array  keys
                    ^number idx]
  ISeqable
  (-seq [this]
    (when keys this))
  
  ISeq
  (-first [_]
    (when keys (aget keys idx)))
  
  (-rest [_]
    (if (< (inc idx) (alength keys))
      ;; can use cached array to move forward
      (BTSetIter. set (inc path) keys (inc idx))
      (let [path (next-path set path)]
        (if (== -1 path)
          (BTSetIter. set -1 nil -1)
          (BTSetIter. set path (keys-for set path) (path-get path 0))))))
  
  INext
  (-next [_]
    (if (< (inc idx) (alength keys))
      ;; can use cached array to move forward
      (BTSetIter. set (inc path) keys (inc idx))
      (let [path (next-path set path)]
        (when (not= -1 path)
          (BTSetIter. set path (keys-for set path) (path-get path 0)))))))

(defn btset-iter [set]
  (when (pos? (alength (.-keys (.-root set))))
    (BTSetIter. set empty-path (keys-for set empty-path) 0)))

;; public interface

(deftype BTSet [root shift comparator]
  Object
  (toString [this]
    (pr-str* this))
  
  ICollection
  (-conj [set key] (btset-conj set key))
  
  ILookup 
  (-lookup [set k]
    (-lookup set k nil))
  (-lookup [set k not-found]
    (let [found (key-for set (seek set k))]
      (if (eq found k) found not-found)))

  ISeqable
  (-seq [this]
    (btset-iter this))
  
  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "#{" " " "}" opts (seq this))))

(defn btset-by
  ([cmp] (BTSet. (LeafNode. (array)) 0 cmp))
  ([cmp & keys]
    (reduce -conj (btset-by cmp) keys)))

(defn btset
  ([] (btset-by compare))
  ([& keys]
    (reduce -conj (btset) keys)))

;; helpers

(defn parse-path [path shift]
  (loop [level shift
         res   []]
    (if (pos? level)
      (recur (- level level-shift)
             (conj res (path-get path level))))))

(defn seek-h [set key]
  (parse-path (seek set key) (.-shift set)))

;; (def s0 (apply btset (range 20000)))
;; (def s1 (into (btset) (range 3)))
;; (apply btset (range 150))


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

;; (extend-type BTSet
;;   IPrintWithWriter
;;   (-pr-writer [o writer _]
;;     (dump (.-root o) writer "")))

(defn test-rand []
  (dotimes [i 100]
    (let [xs (range (rand-int 10000))
          xss (shuffle xs)
          s  (into (btset) xss)]
      (when-not (= (vec s) xs)
        (println xss))))
  (println "Checked")
  :ok)

;; (test-rand)

;; perf

(def test-matrix [:target  { ;; "sorted-set" (sorted-set)
                             "btset"      (btset)}
;;                   :size    [100 500 1000 2000 5000 10000 20000 50000]
                  :size    [100 500 20000]
                  :method  { "conj"    (fn [opts] (into (:target opts) (:range opts)))
                             "lookup"  (fn [opts] (contains? (:set opts) (rand-int (:size opts))))
                             "iterate" (fn [opts] (doseq [x (:set opts)] (+ 1 x)))
                            }])

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
