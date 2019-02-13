(ns datascript.test.btset
  (:require
    [datascript.btset :as btset]
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]]))
  #?(:clj
     (:import [clojure.lang IReduce])))


#?(:cljs (enable-console-print!))

;; confirm that clj's use of sorted set works as intended.
;; allow for [:foo nil] to glob [:foo *]; data will never be inserted
;; w/ nil, but slice/subseq elements will.


(def iters 5)


(defn cmp [x y]
  (if (and x y)
    (compare x y)
    0))


(defn cmp-s [[x0 x1] [y0 y1]]
  (let [c0 (cmp x0 y0)
        c1 (cmp x1 y1)]
    (cond
      (= c0 0) c1
      (< c0 0) -1
      (> c0 0)  1)))


(deftest semantic-test-btset-by
  (let [e0 (btset/btset-by cmp-s)
        ds [[:a :b] [:b :x] [:b :q] [:a :d]]
        e1 (reduce conj e0 ds)]
    (is (= (count ds)        (count (seq e1))))
    (is (= (vec (seq e1))    (vec (btset/slice e1 [nil nil]))))        ; * *
    (is (= [[:a :b] [:a :d]] (vec (btset/slice e1 [:a nil]))))         ; :a *
    (is (= [[:b :q]]         (vec (btset/slice e1 [:b :q]))))          ; :b :q (specific)
    (is (= [[:a :d] [:b :q]] (vec (btset/slice e1 [:a :d] [:b :q]))))  ; matching subrange
    (is (= [[:a :d] [:b :q]] (vec (btset/slice e1 [:a :c] [:b :r]))))  ; non-matching subrange
    (is (= [[:b :x]]         (vec (btset/slice e1 [:b :r] [:c nil])))) ; non-matching -> out of range
    (is (= []                (vec (btset/slice e1 [:c nil]))))         ; totally out of range
    ))

(defn irange [from to]
  (if (< from to)
    (range from (inc to))
    (range from (dec to) -1)))

(deftest test-slice
  (dotimes [i iters]
    (testing "straight 3 layers"
      (let [s (into (btset/btset) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (btset/slice s from to))
          #?@(:clj [
               nil    nil    (irange 0 5000)
               
               -1     nil    (irange 0 5000)
               0      nil    (irange 0 5000)
               0.5    nil    (irange 1 5000)
               1      nil    (irange 1 5000)
               4999   nil    [4999 5000]
               4999.5 nil    [5000]
               5000   nil    [5000]
               5000.5 nil    nil
               
               nil    -1     nil
               nil    0      [0]
               nil    0.5    [0]
               nil    1      [0 1]
               nil    4999   (irange 0 4999)
               nil    4999.5 (irange 0 4999)
               nil    5000   (irange 0 5000)
               nil    5001   (irange 0 5000)
          ])

          -2     -1     nil
          -1     5001   (irange 0 5000)
          0      5000   (irange 0 5000)
          0.5    4999.5 (irange 1 4999)
          2499.5 2500.5 [2500]
          2500   2500   [2500]
          2500.1 2500.9 nil
          5001   5002   nil)))

    (testing "straight 1 layer, leaf == root"
      (let [s (into (btset/btset) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (btset/slice s from to))
          #?@(:clj [
               nil  nil  (irange 0 10)
               
               -1   nil  (irange 0 10)
               0    nil  (irange 0 10)
               0.5  nil  (irange 1 10)
               1    nil  (irange 1 10)
               9    nil  [9 10]
               9.5  nil  [10]
               10   nil  [10]
               10.5 nil  nil
               
               nil -1   nil
               nil 0    [0]
               nil 0.5  [0]
               nil 1    [0 1]
               nil 9    (irange 0 9)
               nil 9.5  (irange 0 9)
               nil 10   (irange 0 10)
               nil 11   (irange 0 10)
          ])

          -2   -1  nil
          -1   10  (irange 0 10)
          0    10  (irange 0 10)
          0.5  9.5 (irange 1 9)
          4.5  5.5 [5]
          5    5   [5]
          5.1  5.9 nil
          11   12  nil)))

    (testing "reverse 3 layers"
      (let [s (into (btset/btset) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (btset/rslice s from to))
          #?@(:clj [
               nil    nil    (irange 5000 0)
               
               5001   nil    (irange 5000 0)
               5000   nil    (irange 5000 0)
               4999.5 nil    (irange 4999 0)
               4999   nil    (irange 4999 0)
               1      nil    [1 0]
               0.5    nil    [0]
               0      nil    [0]
               -1     nil    nil
               
               nil    5001   nil
               nil    5000   [5000]
               nil    4999.5 [5000]
               nil    4999   [5000 4999]
               nil    1      (irange 5000 1)
               nil    0.5    (irange 5000 1)
               nil    0      (irange 5000 0)
               nil    -1     (irange 5000 0)
          ])

          5002   5001   nil
          5001   -1     (irange 5000 0)
          5000   0      (irange 5000 0)
          4999.5 0.5    (irange 4999 1)
          2500.5 2499.5 [2500]
          2500   2500   [2500]
          2500.9 2500.1 nil
          -1     -2     nil)))

    (testing "reverse 1 layer, leaf == root"
      (let [s (into (btset/btset) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (btset/rslice s from to))
          #?@(:clj [
               nil nil (irange 10 0)
               
               11  nil (irange 10 0)
               10  nil (irange 10 0)
               9.5 nil (irange 9 0)
               9   nil (irange 9 0)
               1   nil [1 0]
               0.5 nil [0]
               0   nil [0]
               -1  nil nil
               
               nil 11  nil
               nil 10  [10]
               nil 9.5 [10]
               nil 9   [10 9]
               nil 1   (irange 10 1)
               nil 0.5 (irange 10 1)
               nil 0   (irange 10 0)
               nil -1  (irange 10 0)
          ])

          12  11  nil
          11  -1  (irange 10 0)
          10  0   (irange 10 0)
          9.5 0.5 (irange 9 1)
          5.5 4.5 [5]
          5   5   [5]
          5.9 5.1 nil
          -1  -2  nil)))

    (testing "seq-rseq equivalence"
      (let [s (into (btset/btset) (shuffle (irange 0 5000)))]
        (are [from to] (= (btset/slice s from to) (some-> (btset/slice s from to) (rseq) (reverse)))
          #?@(:clj [
               -1     nil
               0      nil
               2500   nil
               5000   nil
               5001   nil
               
               nil    -1
               nil    0     
               nil    1     
               nil    2500
               nil    5000
               nil    5001  
               
               nil    nil
          ])

          -1     5001
          0      5000  
          1      4999
          2500   2500
          2500.1 2500.9)))

    (testing "rseq-seq equivalence"
      (let [s (into (btset/btset) (shuffle (irange 0 5000)))]
        (are [from to] (= (btset/rslice s from to) (some-> (btset/rslice s from to) (rseq) (reverse)))
          #?@(:clj [
               -1     nil
               0      nil
               2500   nil
               5000   nil
               5001   nil
               
               nil    -1
               nil    0     
               nil    1     
               nil    2500
               nil    5000
               nil    5001  
               
               nil    nil
          ])
          5001   -1    
          5000   0       
          4999   1     
          2500   2500  
          2500.9 2500.1)))

    (testing "Slice with equal elements"
      (let [cmp10 (fn [a b] (compare (quot a 10) (quot b 10)))
            s10   (reduce #(btset/btset-conj %1 %2 compare) (btset/btset-by cmp10) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (btset/slice s10 from to))
          30 30      (irange 30 39)
          130 4970   (irange 130 4979)
          -100 6000  (irange 0 5000))
        (are [from to expected] (= expected (btset/rslice s10 from to))
          30 30      (irange 39 30)
          4970 130   (irange 4979 130)
          6000 -100  (irange 5000 0)))

      (let [cmp100 (fn [a b] (compare (quot a 100) (quot b 100)))
            s100   (reduce #(btset/btset-conj %1 %2 compare) (btset/btset-by cmp100) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (btset/slice s100 from to))
          30  30     (irange 0 99)
          2550 2550  (irange 2500 2599)
          130 4850   (irange 100 4899)
          -100 6000  (irange 0 5000))
        (are [from to expected] (= expected (btset/rslice s100 from to))
          30 30      (irange 99 0)
          2550 2550  (irange 2599 2500)
          4850 130   (irange 4899 100)
          6000 -100  (irange 5000 0))))
))


(defn ireduce
  ([f coll] (#?(:clj .reduce :cljs -reduce) ^IReduce coll f))
  ([f val coll] (#?(:clj .reduce :cljs -reduce) ^IReduce coll f val)))


(defn reduce-chunked [f val coll]
  (if-some [s (seq coll)]
    (if (chunked-seq? s)
      (recur f (#?(:clj .reduce :cljs -reduce) (chunk-first s) f val) (chunk-next s))
      (recur f (f val (first s)) (next s)))
    val))


(deftest test-reduces
  (testing "IReduced"
    (testing "Empty"
      (let [s (btset/btset)]
        (is (= 0 (ireduce + s)))
        (is (= 0 (ireduce + 0 s)))))

    (testing "~3 layers"
      (let [s (into (btset/btset) (irange 0 5000))]
        (is (= 12502500 (ireduce +   s)))
        (is (= 12502500 (ireduce + 0 s)))
        (is (= 12502500 (ireduce +   (seq s))))
        (is (= 12502500 (ireduce + 0 (seq s))))
        (is (= 7502500  (ireduce +   (btset/slice s 1000 4000))))
        (is (= 7502500  (ireduce + 0 (btset/slice s 1000 4000))))
        #?@(:clj [(is (= 12502500 (ireduce +   (rseq s))))
                  (is (= 12502500 (ireduce + 0 (rseq s))))
                  (is (= 7502500  (ireduce +   (btset/rslice s 4000 1000))))
                  (is (= 7502500  (ireduce + 0 (btset/rslice s 4000 1000))))])))

    (testing "~1 layer"
      (let [s (into (btset/btset) (irange 0 10))]
        (is (= 55 (ireduce +   s)))
        (is (= 55 (ireduce + 0 s)))
        (is (= 55 (ireduce +   (seq s))))
        (is (= 55 (ireduce + 0 (seq s))))
        (is (= 35 (ireduce +   (btset/slice s 2 8))))
        (is (= 35 (ireduce + 0 (btset/slice s 2 8))))
        #?@(:clj [(is (= 55 (ireduce +   (rseq s))))
                  (is (= 55 (ireduce + 0 (rseq s))))
                  (is (= 35 (ireduce +   (btset/rslice s 8 2))))
                  (is (= 35 (ireduce + 0 (btset/rslice s 8 2))))]))))

  (testing "IChunkedSeq"
    (testing "~3 layers"
      (let [s (into (btset/btset) (irange 0 5000))]
        (is (= 12502500 (reduce-chunked + 0 s)))
        (is (= 7502500  (reduce-chunked + 0 (btset/slice s 1000 4000))))
        (is (= 12502500 (reduce-chunked + 0 (rseq s))))
        (is (= 7502500  (reduce-chunked + 0 (btset/rslice s 4000 1000))))))

    (testing "~1 layer"
      (let [s (into (btset/btset) (irange 0 10))]
        (is (= 55 (reduce-chunked + 0 s)))
        (is (= 35 (reduce-chunked + 0 (btset/slice s 2 8))))
        (is (= 55 (reduce-chunked + 0 (rseq s))))
        (is (= 35 (reduce-chunked + 0 (btset/rslice s 8 2))))))))


(defn into-via-doseq [to from]
  (let [res (transient [])]
    (doseq [x from]  ;; checking chunked iter
      (conj! res x))
    (persistent! res)))


(deftest stresstest-btset
  (dotimes [i iters]
    (let [xs        (vec (repeatedly (+ 1 (rand-int 10000)) #(rand-int 10000)))
          xs-sorted (vec (distinct (sort xs)))
          rm        (vec (repeatedly (rand-int 50000) #(rand-nth xs)))
          full-rm   (shuffle (concat xs rm))
          xs-rm     (reduce disj (into (sorted-set) xs) rm)]
      #_(println "Checking btset" (str (inc i)  "/" iters ":")
               (count xs) "adds" (str "(" (count xs-sorted) " distinct),")
               (count rm) "removals" (str "(down to " (count xs-rm) ")"))
      (doseq [[method set0] [["conj" (into (btset/btset) xs)]
                             ["bulk" (apply btset/btset xs)]]
              :let [set1 (reduce disj set0 rm)
                    set2 (reduce disj set0 full-rm)]]
        (testing method
          (testing "conj, seq"
            (is (= (vec set0) xs-sorted)))
          (testing "eq"
            (is (= set0 (set xs-sorted))))
          (testing "count"
            (is (= (count set0) (count xs-sorted))))
          (testing "doseq"
            (is (= (into-via-doseq [] set0) xs-sorted)))
          (testing "disj"
            (is (= (vec set1) (vec xs-rm)))
            (is (= (count set1) (count xs-rm)))
            (is (= set1 xs-rm))
            (is (= set2 #{}))))
        )))
  #_(println "[ OK ] btset checked"))


(deftest stresstest-slice
  (dotimes [i iters]
    (let [xs        (repeatedly (+ 1 (rand-int 20000)) #(rand-int 20000))
          xs-sorted (distinct (sort xs))
          [from to] (sort [(- 10000 (rand-int 20000)) (+ 10000 (rand-int 20000))])
          expected  (filter #(<= from % to) xs-sorted)
;;             _         (println "Checking btset/slice" (str (inc i)  "/" iters)
;;                                "from" (count xs-sorted) "elements down to" (count expected))
          set       (into (btset/btset) xs)
          set-range (btset/slice set from to)]
        (testing (str "from " from " to " to)
          (is (= (vec set-range) (vec (seq set-range)))) ;; checking IReduce on BTSetIter
          (is (= (vec set-range) expected))
          (is (= (into-via-doseq [] set-range) expected))
          (is (= (vec (rseq set-range)) (reverse expected)))
          (is (= (vec (rseq (rseq set-range))) expected))
          )))
#_(println "[ OK ] btset slice checked"))


;; (t/test-ns 'datascript.test.btset)
