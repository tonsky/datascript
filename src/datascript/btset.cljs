(ns datascript.btset
  (:require [test.datascript.perf :as perf]))

(declare BTSet Node LeafNode)

(def ^:const ^number min-len 64)
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
                           ^array  xs] ;; TODO slice + concat?
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

(defn cut ;; TODO use Array.prototype.slice etc
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

(defn ^array merge-n-split [^array a1
                            ^array a2]
  (let [a1-l    (alength a1)
        a2-l    (alength a2)
        total-l (+ a1-l a2-l)
        r1-l    (half total-l)
        r2-l    (- total-l r1-l)
        r1      (make-array r1-l)
        r2      (make-array r2-l)]
    (dotimes [i total-l]
      (aset (if (< i r1-l) r1 r2)
            (if (< i r1-l) i (- i r1-l))
            (aget (if (< i a1-l) a1 a2)
                  (if (< i a1-l) i (- i a1-l)))))
    #js [r1 r2]))

(defn ^boolean eq-arr [^array  a1
                       ^number a1-from
                       ^number a1-to
                       ^array  a2
                       ^number a2-from
                       ^number a2-to
                       cmp]
  (let [len (- a1-to a1-from)]
    (and
      (== len (- a2-to a2-from))
      (loop [i 0]
        (cond
          (== i len)
            true
          (not (cmp (aget a1 (+ i a1-from))
                    (aget a2 (+ i a2-from))))
            false
          :else
            (recur (inc i)))))))

(defn ^array check-n-splice [^array  arr
                             ^number from
                             ^number to
                             ^array  new-arr]
  (if (eq-arr arr from to new-arr 0 (alength new-arr) eq)
    arr
    (splice arr from to new-arr)))

;; 

(defn lim-key [node]
  (aget (.-keys node) (dec (alength (.-keys node)))))

(defn return-array
  "Drop non-nil references and return array of arguments"
  ([a1] #js [a1])
  ([a1 a2] (if a1 (if a2 #js [a1 a2] #js [a1]) #js [a2]))
  ([a1 a2 a3] (if a1 (if a2 (if a3 #js [a1 a2 a3] #js [a1 a2]) (if a3 #js [a1 a3] #js [a1])) (if a2 (if a3 #js [a2 a3] #js [a2]) #js [a3]))))

(defn rotate [node root? left right]
  (cond
    ;; root never merges
    root?
      (return-array node)

    ;; enough keys, nothing to merge
    (> (.len node) min-len)
      (return-array left node right)

    ;; left and this can be merged to one
    (and left (<= (.len left) min-len))
      (return-array (.merge left node) right)

    ;; right and this can be merged to one
    (and right (<= (.len right) min-len))
      (return-array left (.merge node right))

    ;; left has fewer nodes, redestribute with it
    (and left (or (nil? right)
                  (< (.len left) (.len right))))
      (let [nodes (.merge-n-split left node)]
        (return-array (aget nodes 0) (aget nodes 1) right))

    ;; right has fewer nodes, redestribute with it
    :else
      (let [nodes (.merge-n-split node right)]
        (return-array left (aget nodes 0) (aget nodes 1)))))

(deftype Node [keys pointers]
  Object
  (len [_]
    (alength keys))
  
  (merge [_ next]
    (Node. (.concat keys (.-keys next))
           (.concat pointers (.-pointers next))))
  
  (merge-n-split [_ next]
    (let [ks (merge-n-split keys (.-keys next))
          ps (merge-n-split pointers (.-pointers next))]
      (return-array (Node. (aget ks 0) (aget ps 0))
                    (Node. (aget ks 1) (aget ps 1)))))
  
  (lookup [_ key]
    (let [idx (binary-search keys 0 (dec (alength keys)) key)]
      (if (== idx (alength keys))
        -1
        idx)))
  
  (append [this path level key] ;; TODO figure out idx here
    (let [idx   (path-get path level)
          nodes (.append (aget pointers idx) path (- level level-shift) key)]
      (if (identical? (aget nodes 0) (aget pointers idx))
        #js [this]
        (let [new-keys     (check-n-splice keys     idx (inc idx) (.map nodes lim-key))
              new-pointers (splice         pointers idx (inc idx) nodes)]
          (if (<= (alength new-pointers) max-len)
          ;; ok as is
          #js [(Node. new-keys new-pointers)]
          ;; gotta split it up
          (let [middle  (half (alength new-pointers))]
            #js [(Node. (cut new-keys     0 middle)
                        (cut new-pointers 0 middle))
                 (Node. (cut new-keys     middle)
                        (cut new-pointers middle))]))))))

  (disj [this ^number key root? left right]
    (let [idx (.lookup this key)]
      (when-not (== -1 idx) ;; short-circuit, key not here
        (let [child       (aget pointers idx)
              left-child  (when (>= (dec idx) 0)                 (aget pointers (dec idx)))
              right-child (when (< (inc idx) (alength pointers)) (aget pointers (inc idx)))
              disjned     (.disj child key false left-child right-child)]
          (when disjned     ;; short-circuit, key not here
            (let [left-idx     (if left-child  (dec idx) idx)
                  right-idx    (if right-child (+ 2 idx) (+ 1 idx))
                  new-keys     (check-n-splice keys left-idx right-idx (.map disjned lim-key))
                  new-pointers (splice pointers left-idx right-idx disjned)]
              (rotate (Node. new-keys new-pointers) root? left right))))))))

(deftype LeafNode [keys]
  Object
  (toString [_] (pr-str* (vec keys)))
  
  (len [_]
    (alength keys))
  
  (merge [_ next]
    (LeafNode. (.concat keys (.-keys next))))
  
  (merge-n-split [_ next]
    (let [ks (merge-n-split keys (.-keys next))]
      (return-array (LeafNode. (aget ks 0))
                    (LeafNode. (aget ks 1)))))
  
  (lookup [_ key]
    (let [idx (binary-search keys 0 (dec (alength keys)) key)]
      (if (and (< idx (alength keys))
                 (eq (aget keys idx) key))
        idx
        -1)))

  (append [this ^number path _ key] ;; TODO figure out idx here
    (let [idx    (path-get path 0)
          keys-l (alength keys)]
      (cond
        (eq key (aget keys idx)) ;; element already there
          #js [this]
        (== keys-l max-len) ;; splitting
          (let [middle (half (inc keys-l))]
            (if (> idx middle)
              ;; new key goes to the second half
              #js [(LeafNode. (cut keys 0 middle))
                   (LeafNode. (cut-n-splice keys middle keys-l idx idx #js [key]))]
              ;; new key goes to the first half
              #js [(LeafNode. (cut-n-splice keys 0 middle idx idx #js [key]))
                   (LeafNode. (cut keys middle keys-l))]))
        :else ;; ok as is
          #js [(LeafNode. (splice keys idx idx #js [key]))])))
  
  (disj [this ^number key root? left right]
    (let [idx (.lookup this key)]
      (when-not (== -1 idx) ;; key not there
        (let [new-keys (splice keys idx (inc idx) #js [])]
          (rotate (LeafNode. new-keys) root? left right))))))

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

(defn btset-disj [set key]
  (binding [*cmp* (.-comparator set)]
    (let [new-roots (.disj (.-root set) key true nil nil)]
      (if (nil? new-roots) ;; nothing changed, key wasn't in the set
        set
        (let [new-root (aget new-roots 0)]
          (if (and (instance? Node new-root)
                   (== (alength (.-pointers new-root)) 1))
            ;; root has one child, make him new root
            (BTSet. (aget (.-pointers new-root) 0) (- (.-shift set) level-shift) *cmp*)
            ;; keeping root level
            (BTSet. new-root (.-shift set) *cmp*)))))))

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
;;   (toString [this]
;;     (pr-str* this))
  
  ICollection
  (-conj [set key] (btset-conj set key))

  ISet
  (-disjoin [set key]
    (btset-disj set key))
  
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

(extend-type BTSet
  IPrintWithWriter
  (-pr-writer [o writer _]
    (dump (.-root o) writer "")))

;; (def s0 (into (btset) (shuffle (range 10000))))
;; (reduce disj s0 (shuffle (range 9995)))

(defn test-rand []
  (dotimes [i 20]
    (let [xs        (vec (repeatedly (rand-int 10000) #(rand-int 10000)))
          xs-sorted (distinct (sort xs))
          set0      (into (btset) xs)
          rm        (repeatedly (rand-int 10000) #(rand-nth xs))
          set1      (reduce disj set0 rm)]          
      (when-not (= (vec set0) xs-sorted)
        (throw (js/Error. (str "conj failed on: " xs))))
      (when-not (= (vec set1) (->> (reduce disj (into #{} xs) rm) sort))
        (throw (js/Error. (str "disj failed on: " xs " and " rm))))
      (println "Checking...")))
  (println "Checked")
  :ok)

;; (test-rand)

;; perf

(def test-matrix [:target    { ;; "sorted-set" (sorted-set)
                               "btset"      (btset)}
                  ;; :distinct? [true false]
;;                   :size    [100 500 1000 2000 5000 10000 20000 50000]
                  :size      [100 500 20000]
                  :method    { "conj"    (fn [opts] (into (:target opts) (:range opts)))
;;                                "lookup"  (fn [opts] (contains? (:set opts) (rand-int (:size opts))))
;;                                "iterate" (fn [opts] (doseq [x (:set opts)] (+ 1 x)))
                             }])

(defn test-setup [opts]
  (let [xs (if (:disticnt? opts true)
             (shuffle (range (:size opts)))
             (repeatedly (:size opts) #(rand-int (:size opts))))]
    (-> opts
        (assoc :range xs)
        (assoc :set (into (:target opts) xs)))))

(defn ^:export perftest []
  (perf/suite (fn [opts] ((:method opts) opts))
    :duration 1000
    :matrix   test-matrix
    :setup-fn test-setup))

;; (perftest)
