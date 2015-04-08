(ns ^{:doc
" B+ tree
  -------

  LeafNode: keys[]     :: array of values

  Node:     pointers[] :: links to children nodes
            keys[]     :: max value for whole subtree
                          node.keys[i] == max(node.pointers[i].keys)
  All arrays are 64..128 elements, inclusive

  BTSet:    root       :: Node or LeafNode
            shift      :: path bit-shift of root level, == (depth - 1) * level-shift
            cnt        :: size of a set, integer, rolling
            comparator :: comparator used for ordering
            meta       :: clojure meta map
            __hash     :: hash code, same as for clojure collections, on-demand, cached

  Path: conceptually a vector of indexes from root to leaf value, but encoded in a single int.
        E.g. we have path [7 53 11] representing root.pointers[7].pointers[3].keys[11].
        In our case level-shift is 8, meaning each index will take 8 bits:
        (7 << 16) | (53 << 8) | 11 = 472331
        0000 0111   0011 0101   0000 1011

  BTSetIter: set       :: Set this iterator belongs to
             left      :: Current path
             right     :: Right bound path (exclusive)
             keys      :: Cached ref for keys array for a leaf
             idx       :: Cached idx in keys array
  Keys and idx are cached for fast iteration inside a leaf
"
  :author "Nikita Prokopov"}
  datascript.btset)

(declare BTSet Node LeafNode)

(def ^:const min-len 64)
(def ^:const max-len 128)
(def ^:const level-shift (->> (range 31 -1 -1)
                              (filter #(bit-test max-len %))
                              first
                              inc))
(def ^:const path-mask (dec (bit-shift-left 1 level-shift)))
(def ^:const empty-path 0)
(def ^:dynamic *cmp*)

(defn path-get [path level]
  (bit-and path-mask
           (unsigned-bit-shift-right path level)))

(defn path-set [path level idx]
  (bit-or path 
          (bit-shift-left idx level)))

(defn eq [a b]
  (== 0 (*cmp* a b)))

(defn half [x]
  (unsigned-bit-shift-right x 1))

(defn binary-search-l [arr l r k]
  (if (<= l r)
    (let [m   (half (+ l r))
          mk  (aget arr m)
          cmp (*cmp* mk k)]
      (if (neg? cmp)
        (recur arr (inc m) r k)
        (recur arr l (dec m) k)))
    l))

(defn binary-search-r [arr l r k]
  (if (<= l r)
    (let [m   (half (+ l r))
          mk  (aget arr m)
          cmp (*cmp* mk k)]
      (if (pos? cmp)
        (recur arr l (dec m) k)
        (recur arr (inc m) r k)))
    l))

(defn lookup-exact [arr key]
  (let [arr-l (alength arr)
        idx   (binary-search-l arr 0 (dec arr-l) key)]
    (if (and (< idx arr-l)
             (eq (aget arr idx) key))
      idx
      -1)))

(defn lookup-range [arr key]
  (let [arr-l (alength arr)
        idx   (binary-search-l arr 0 (dec arr-l) key)]
    (if (== idx arr-l)
      -1
      idx)))

;; Array operations

(defn alast [arr]
  (aget arr (dec (alength arr))))

(defn cut-n-splice [arr cut-from cut-to splice-from splice-to xs]
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
  ([arr cut-from]
    (.slice arr cut-from))
  ([arr cut-from cut-to]
    (.slice arr cut-from cut-to)))

(defn splice [arr splice-from splice-to xs]
  (cut-n-splice arr 0 (alength arr) splice-from splice-to xs))

(defn insert [arr idx xs]
  (cut-n-splice arr 0 (alength arr) idx idx xs))

(defn merge-n-split [a1 a2]
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

(defn ^boolean eq-arr [a1 a1-from a1-to a2 a2-from a2-to cmp]
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

(defn check-n-splice [arr from to new-arr]
  (if (eq-arr arr from to new-arr 0 (alength new-arr) eq)
    arr
    (splice arr from to new-arr)))

(defn arr-map-inplace [f arr]
  (let [len (alength arr)]
    (loop [i 0]
      (when (< i len)
        (aset arr i (f (aget arr i)))
        (recur (inc i))))
    arr))

(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2"
  [min-len max-len arr]
  (let [chunk-len (half (+ max-len min-len))
        len       (alength arr)
        acc       #js []]
    (when (pos? len)
      (loop [pos 0]
        (let [rest (- len pos)]
          (cond
            (<= rest max-len)
              (.push acc (.slice arr pos))
            (>= rest (+ chunk-len min-len))
              (do
                (.push acc (.slice arr pos (+ pos chunk-len)))
                (recur (+ pos chunk-len)))
            :else
              (let [piece-len (half rest)]
                (.push acc (.slice arr pos (+ pos piece-len)))
                (recur (+ pos piece-len)))))))
    acc))

(defn- arr-distinct
  "Filter out repetitive values in a sorted array"
  [arr cmp]
  (loop [i 0]
    (if (>= i (alength arr))
      arr
      (if (and (> i 0)
               (== 0 (cmp (aget arr i) (aget arr (dec i)))))
        (do
          (.splice arr i 1)
          (recur i))
        (recur (inc i)))))
  arr)

;; 

(defn lim-key [node]
  (alast (.-keys node)))

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
    (let [idx (lookup-range keys key)]
      (when-not (== -1 idx)
        (.lookup (aget pointers idx) key))))
  
  (conj [this key]
    (let [idx   (binary-search-l keys 0 (- (alength keys) 2) key)
          nodes (.conj (aget pointers idx) key)]
      (when nodes
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

  (disj [this key root? left right]
    (let [idx (lookup-range keys key)]
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
    (let [idx (lookup-exact keys key)]
      (when-not (== -1 idx)
        (aget keys idx))))

  (conj [this key]
    (let [idx    (binary-search-l keys 0 (dec (alength keys)) key)
          keys-l (alength keys)]
      (cond
        ;; element already here
        (and (< idx keys-l)
             (eq key (aget keys idx)))
          nil
      
        ;; splitting
        (== keys-l max-len)
          (let [middle (half (inc keys-l))]
            (if (> idx middle)
              ;; new key goes to the second half
              #js [(LeafNode. (cut keys 0 middle))
                   (LeafNode. (cut-n-splice keys middle keys-l idx idx #js [key]))]
              ;; new key goes to the first half
              #js [(LeafNode. (cut-n-splice keys 0 middle idx idx #js [key]))
                   (LeafNode. (cut keys middle keys-l))]))
       
        ;; ok as is
        :else
          #js [(LeafNode. (splice keys idx idx #js [key]))])))
  
  (disj [this key root? left right]
    (let [idx (lookup-exact keys key)]
      (when-not (== -1 idx) ;; key is here
        (let [new-keys (splice keys idx (inc idx) #js [])]
          (rotate (LeafNode. new-keys) root? left right))))))

(defn keys-for [set path]
  (loop [level (.-shift set)
         node  (.-root set)]
    (if (pos? level)
      (recur (- level level-shift) (aget (.-pointers node) (path-get path level)))
      (.-keys node))))

(declare alter-btset)

(defn btset-conj [set key cmp]
  (binding [*cmp* cmp]
    (let [roots (.conj (.-root set) key)]
      (cond
        ;; tree not changed
        (nil? roots)
          set
       
        ;; keeping single root
        (== (alength roots) 1)
          (alter-btset set
            (aget roots 0)
            (.-shift set)
            (inc (.-cnt set)))
       
        ;; introducing new root
        :else
          (alter-btset set
            (Node. (.map roots lim-key) roots)
            (+ (.-shift set) level-shift)
            (inc (.-cnt set)))))))

(defn btset-disj [set key cmp]
  (binding [*cmp* cmp]
    (let [new-roots (.disj (.-root set) key true nil nil)]
      (if (nil? new-roots) ;; nothing changed, key wasn't in the set
        set
        (let [new-root (aget new-roots 0)]
          (if (and (instance? Node new-root)
                   (== (alength (.-pointers new-root)) 1))
            
            ;; root has one child, make him new root
            (alter-btset set
              (aget (.-pointers new-root) 0)
              (- (.-shift set) level-shift)
              (dec (.-cnt set)))
            
            ;; keeping root level
            (alter-btset set
              new-root
              (.-shift set)
              (dec (.-cnt set)))))))))

;; iteration

(defn -next-path [node path level]
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

(defn next-path
  "Returns path representing next item after `path` in natural traversal order,
   or -1 if end of tree has been reached"
  [set path]
  (-next-path (.-root set) path (.-shift set)))

(defn -rpath
  "Returns rightmost path possible starting from node and going deeper"
  [node level]
  (loop [node  node
         path  empty-path
         level level]
    (if (pos? level)
      (recur (alast (.-pointers node))
             (path-set path level (dec (alength (.-pointers node))))
             (- level level-shift))
      (path-set path 0 (dec (alength (.-keys node)))))))

(defn -prev-path [node path level]
  (let [idx (path-get path level)]
    (if (pos? level)
      ;; inner node
      (let [sub-level (- level level-shift)
            sub-path  (-prev-path (aget (.-pointers node) idx) path sub-level)]
        (if (== -1 sub-path)
          ;; nested node overflow
          (if (>= (dec idx) 0)
            ;; advance current node idx, reset subsequent indexes
            (let [idx      (dec idx)
                  sub-path (-rpath (aget (.-pointers node) idx) sub-level)]
              (path-set sub-path level idx))
            ;; current node overflow
            -1)
          ;; keep current idx
          (path-set sub-path level idx)))
      ;; leaf
      (if (>= (dec idx) 0)
        ;; advance leaf idx
        (path-set empty-path 0 (dec idx))
        ;; leaf overflow
        -1))))

(defn prev-path
  "Returns path representing previous item before `path` in natural traversal order,
   or -1 if `path` was already beginning of a tree"
  [set path]
  (-prev-path (.-root set) path (.-shift set)))

(declare BTSetBackwardsIter -btset-iter -btset-backwards-iter)

(deftype BTSetIter [set left right keys idx]
  ISeqable
  (-seq [this]
    (when keys this))
  
  ISeq
  (-first [_]
    (when keys (aget keys idx)))
  
  (-rest [this]
    (if-let [next (-next this)]
      next
      (BTSetIter. set -1 -1 nil -1)))
  
  INext
  (-next [_]
    (when keys
      (if (< (inc idx) (alength keys))
        ;; can use cached array to move forward
        (when (< (inc left) right)
          (BTSetIter. set (inc left) right keys (inc idx)))
        (let [left (next-path set left)]
          (when (and (not= -1 left) (< left right))
            (-btset-iter set left right))))))
  
  IReversible
  (-rseq [_]
    (when keys
      (-btset-backwards-iter set (prev-path set left) (prev-path set right)))))

(defn -btset-iter [set left right]
  (BTSetIter. set left right (keys-for set left) (path-get left 0)))

(deftype BTSetBackwardsIter [set left right keys idx]
  ISeqable
  (-seq [this]
    (when keys this))
  
  ISeq
  (-first [_]
    (when keys (aget keys idx)))
  
  (-rest [this]
    (if-let [next (-next this)]
      next
      (BTSetBackwardsIter. set -1 -1 nil -1)))
  
  INext
  (-next [_]
    (when keys
      (if (>= (dec idx) 0)
        ;; can use cached array to advance
        (when (> (dec right) left)
          (BTSetBackwardsIter. set left (dec right) keys (dec idx)))
        (let [right (prev-path set right)]
          (when (and (not= -1 right) (> right left))
            (-btset-backwards-iter set left right))))))
  
  IReversible
  (-rseq [_]
    (when keys
      (let [new-left  (if (== left -1) 0 (next-path set left))
            new-right (next-path set right)
            new-right (if (== new-right -1) (inc right) new-right)]
        (-btset-iter set new-left new-right)))))

(defn -btset-backwards-iter [set left right]
  (BTSetBackwardsIter. set left right (keys-for set right) (path-get right 0)))

(defn btset-iter
  "Iterator that represents whole set"
  [set]
  (when (pos? (alength (.-keys (.-root set))))
    (let [left   empty-path
          right  (inc (-rpath (.-root set) (.-shift set)))]
      (-btset-iter set left right))))

(defn -seek
  "Returns path to first element >= key,
   or -1 if all elements in a set < key"
  [set key]
  (loop [node  (.-root set)
         path  empty-path
         level (.-shift set)]
    (let [keys   (.-keys node)
          keys-l (alength keys)]
      (if (== 0 level)
        (let [idx (binary-search-l keys 0 (dec keys-l) key)]
          (if (== keys-l idx) -1 (path-set path 0 idx)))
        (let [idx (binary-search-l keys 0 (- keys-l 2) key)]
          (recur (aget (.-pointers node) idx)
                 (path-set path level idx)
                 (- level level-shift)))))))

(defn -rseek
  "Returns path to the first element that is > key.
   If all elements in a set are <= key, returns `(-rpath set) + 1`.
   It’s a virtual path that is bigger than any path in a tree"
  [set key]
  (loop [node  (.-root set)
         path  empty-path
         level (.-shift set)]
    (let [keys   (.-keys node)
          keys-l (alength keys)]
      (if (== 0 level)
        (let [idx (binary-search-r keys 0 (dec keys-l) key)]
          (path-set path 0 idx))
        (let [idx (binary-search-r keys 0 (- keys-l 2) key)]
          (recur (aget (.-pointers node) idx)
                 (path-set path level idx)
                 (- level level-shift)))))))

(defn -slice [set key-from key-to]
  (let [path (-seek set key-from)]
    (when-not (neg? path)
      (let [till-path (-rseek set key-to)]
        (when (> till-path path)
          (BTSetIter. set path till-path (keys-for set path) (path-get path 0)))))))

(defn slice
  "When called with single key, returns iterator over set that contains all elements equal to the key.
   When called with two keys (range), returns iterator for all X where key-from <= X <= key-to"
  ([set key] (slice set key key))
  ([set key-from key-to]
    (binding [*cmp* (.-comparator set)]
      (-slice set key-from key-to))))

;; public interface

(deftype BTSet [root shift cnt comparator meta ^:mutable __hash]
  Object
  (toString [this]
    (pr-str* this))
  
  ICloneable
  (-clone [_] (BTSet. root shift cnt comparator meta __hash))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. root shift cnt comparator new-meta __hash))

  IMeta
  (-meta [_] meta)
 
  IEmptyableCollection
  (-empty [_] (BTSet. (LeafNode. (array)) 0 0 comparator meta 0))
  
  IEquiv
  (-equiv [this other]
    (and
      (set? other)
      (== cnt (count other))
      (every? #(contains? this %) other)))

  IHash
  (-hash [coll] (caching-hash coll hash-iset __hash))
   
  ICollection
  (-conj [set key] (btset-conj set key comparator))

  ISet
  (-disjoin [set key]
    (btset-disj set key comparator))
  
  ILookup 
  (-lookup [set k]
    (-lookup set k nil))
  (-lookup [_ k not-found]
    (binding [*cmp* comparator]
      (or (.lookup root k) not-found)))

  ISeqable
  (-seq [this]
    (btset-iter this))
  
  IReversible
  (-rseq [this]
    (reverse (btset-iter this)))
  
  ICounted
  (-count [_] cnt)
 
  IFn
  (-invoke [coll k]
    (-lookup coll k))
  (-invoke [coll k not-found]
    (-lookup coll k not-found))
  
  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "#{" " " "}" opts (seq this))))

(defn alter-btset [set root shift cnt]
  (BTSet. root shift cnt (.-comparator set) (.-meta set) nil))

(defn -btset-from-sorted-arr [arr cmp]
  (let [leafs (->> arr
                   (arr-partition-approx min-len max-len)
                   (arr-map-inplace ->LeafNode))]
    (loop [current-level leafs
           shift 0]
      (case (count current-level)
        0 (BTSet. (LeafNode. (array)) 0 0 cmp nil 0)
        1 (BTSet. (first current-level) shift (alength arr) cmp nil 0)
        (recur (->> current-level
                    (arr-partition-approx min-len max-len)
                    (arr-map-inplace #(Node. (.map % lim-key) %)))
               (+ shift level-shift))))))

(defn -btset-from-seq [seq cmp]
  (let [arr (-> seq into-array (.sort cmp) (arr-distinct cmp))]
    (-btset-from-sorted-arr arr cmp)))

(defn btset-by
  ([cmp] (BTSet. (LeafNode. (array)) 0 0 cmp nil 0))
  ([cmp & keys]
    (-btset-from-seq keys cmp)))

(defn btset
  ([] (btset-by compare))
  ([& keys]
    (-btset-from-seq keys compare)))

