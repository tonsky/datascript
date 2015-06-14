(ns datascript.test.btset
  #?@(:cljs
      [(:require-macros [cemerick.cljs.test :refer [is deftest testing]])
       (:require [cemerick.cljs.test :as t]
                 [datascript.btset :as btset :refer [btset btset-by slice]]
                 [datascript.perf :as perf])]
      :clj
      [(:require [clojure.test :as t :refer [is deftest testing]]
                 [datascript.btset :as btset :refer [btset btset-by slice]])]))

#?(:cljs (enable-console-print!))

;; confirm that clj's use of sorted set works as intended.
;; allow for [:foo nil] to glob [:foo *]; data will never be inserted
;; w/ nil, but slice/subseq elements will.

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
  (let [e0 (btset-by cmp-s)
        ds [[:a :b] [:b :x] [:b :q] [:a :d]]
        e1 (reduce conj e0 ds)]
    (is (= (count ds)        (count (seq e1))))
    (is (= (vec (seq e1))    (vec (slice e1 [nil nil]))))        ; * *
    (is (= [[:a :b] [:a :d]] (vec (slice e1 [:a nil]))))         ; :a *
    (is (= [[:b :q]]         (vec (slice e1 [:b :q]))))          ; :b :q (specific)
    (is (= [[:a :d] [:b :q]] (vec (slice e1 [:a :d] [:b :q]))))  ; matching subrange
    (is (= [[:a :d] [:b :q]] (vec (slice e1 [:a :c] [:b :r]))))  ; non-matching subrange
    (is (= [[:b :x]]         (vec (slice e1 [:b :r] [:c nil])))) ; non-matching -> out of range
    (is (= []                (vec (slice e1 [:c nil]))))         ; totally out of range
    ))


(deftest stresstest-btset
  (let [iters 5]
    (dotimes [i iters]
      (let [xs        (vec (repeatedly (rand-int 10000) #(rand-int 10000)))
            xs-sorted (distinct (sort xs))
            rm        (repeatedly (rand-int 50000) #(rand-nth xs))
            xs-rm     (reduce disj (into (sorted-set) xs) rm)]
        #_(println "Checking btset" (str (inc i)  "/" iters ":")
                 (count xs) "adds" (str "(" (count xs-sorted) " distinct),")
                 (count rm) "removals" (str "(down to " (count xs-rm) ")"))
        (doseq [[method set0] [["conj" (into (btset) xs)]
                               ["bulk" (apply btset xs)]]
                :let [set1 (reduce disj set0 rm)]]
          (testing method
            (testing "conj, seq"
              (is (= (vec set0) xs-sorted)))
            (testing "eq"
              (is (= set0 (set xs-sorted))))
            (testing "count"
              (is (= (count set0) (count xs-sorted))))
            (testing rm
              (testing "disj"
                (is (= (vec set1) (vec xs-rm)))
                (is (= (count set1) (count xs-rm)))
                (is (= set1 xs-rm)))))
          ))))
    #_(println "[ OK ] btset checked"))

(deftest stresstest-slice
  (let [iters 5]
    (dotimes [i iters]
      (let [xs        (repeatedly (rand-int 20000) #(rand-int 20000))
            xs-sorted (distinct (sort xs))
            [from to] (sort [(- 10000 (rand-int 20000)) (+ 10000 (rand-int 20000))])
            expected  (filter #(<= from % to) xs-sorted)
;;             _         (println "Checking btset/slice" (str (inc i)  "/" iters)
;;                                "from" (count xs-sorted) "elements down to" (count expected))
            set       (into (btset) xs)
            set-range (slice set from to)]
        (testing xs
          (testing (str "from " from " to " to)
            (is (= (vec set-range) (vec (seq set-range)))) ;; checking IReduce on BTSetIter
            (is (= (vec set-range) expected))
            (is (= (vec (reverse set-range)) (reverse expected)))
            (is (= (vec (reverse (reverse set-range))) expected))
            )))))
;;   (println "[ OK ] btset slice checked")
  )


;; (t/test-ns 'datascript.test.btset)
