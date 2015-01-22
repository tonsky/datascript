(ns datascript.test.btset
  (:require-macros
    [cemerick.cljs.test :refer [is deftest testing]])
  (:require
    [datascript.btset :as btset :refer [btset btset-iter -btset-from-sorted-arr slice LeafNode]]
    [cemerick.cljs.test :as t]
    [datascript.perf :as perf]))

(enable-console-print!)

;; helpers


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

(defn decode-path [path]
  (cond
    (== path -1) -1
    (== path 0)  [0]
    :else
    (loop [path path
           acc  ()]
      (if (== 0 path)
        (vec acc)
        (recur
          (unsigned-bit-shift-right path 8)
          (conj acc (bit-and path 0xFF)))))))

(deftest stresstest-btset
  (let [iters 5]
    (dotimes [i iters]
      (let [xs        (vec (repeatedly (rand-int 10000) #(rand-int 10000)))
            xs-sorted (distinct (sort xs))
            rm        (repeatedly (rand-int 50000) #(rand-nth xs))
            xs-rm     (reduce disj (into (sorted-set) xs) rm)
            _         (println "Checking btset" (str (inc i)  "/" iters ":")
                               (count xs) "adds" (str "(" (count xs-sorted) " distinct),")
                               (count rm) "removals" (str "(down to " (count xs-rm) ")"))]
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
    (println "[ OK ] btset checked"))

(deftest stresstest-slice
  (let [iters 5]
    (dotimes [i iters]
      (let [xs        (repeatedly (rand-int 20000) #(rand-int 20000))
            xs-sorted (distinct (sort xs))
            [from to] (sort [(- 10000 (rand-int 20000)) (+ 10000 (rand-int 20000))])
            expected  (filter #(<= from % to) xs-sorted)
            _         (println "Checking btset/slice" (str (inc i)  "/" iters)
                               "from" (count xs-sorted) "elements down to" (count expected))
            set       (into (btset) xs)
            set-range (slice set from to)]
        (testing xs
          (testing (str "from " from " to " to)
            (is (= (vec set-range) expected))
            (is (= (vec (reverse set-range)) (reverse expected)))
            (is (= (vec (reverse (reverse set-range))) expected))
            )))))
  (println "[ OK ] btset slice checked"))

;; (t/test-ns 'datascript.test.btset)

;;;; PERFORMANCE

(def test-matrix [:target    { "sorted-set" (sorted-set)
                               "btset"      (btset)}
;;                   :distinct? [true false]
;;                   :size    [100 500 1000 2000 5000 10000 20000 50000]
                  :size      [100 500 20000]
                  :method    { "conj"    (fn [opts] (into (:target opts) (:range opts)))
                               "disj"    (fn [opts] (reduce disj (:set opts) (shuffle (:range opts))))
                               "lookup"  (fn [opts] (contains? (:set opts) (rand-int (:size opts))))
                               "iterate" (fn [opts] (doseq [x (:set opts)] (+ 1 x)))
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

(defn ^:export perftest-bulk []
  (perf/suite (fn [opts]
                (let [f (:fn opts)
                      s (repeatedly (:size opts) #(rand-int (:size opts)))]
                  (f s (-> s into-array (.sort)))))
    :duration 1000
    :matrix   [:size [100 500 5000 20000]
               :fn   {"bulk"        (fn [seq _] (apply btset seq))
;;                       "bulk-sorted" (fn [_ sorted-arr] (btset-from-sorted-arr sorted-arr compare))
                      "conj"        (fn [seq _] (into (btset) seq))
                      
                      }]))
       
;; (perftest)
;; (perftest-bulk)

;; (.log js/console (apply btset (range 129)))
