(ns test.datascript-perf
  (:require-macros
    [cemerick.cljs.test :refer (is deftest with-test run-tests testing test-var)])
  (:require
    [datascript.btset :as btset]
    [cemerick.cljs.test :as t]
    [datascript :as d]
    [datascript.query :as dq]
    [test.datascript.perf :as perf]))

(enable-console-print!)

;; Performance

(defn random-man []
  {:name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex       (rand-nth [:male :female])
   :age       (rand-int 10)
   :salary    (rand-int 100000)})

(def test-matrix-transact [ :test   ["transact"]
                            :size   [100 500 2000]
                            :batch  [1 5] ])

(defn test-setup-people [opts]
  (let [people (repeatedly (:size opts) random-man)
        people (mapv #(assoc %1 :db/id (- -1 %2)) people (range))]
    (assoc opts
      :people people)))

(defn ^:export perftest-transact []
  (perf/suite (fn [opts]
                (let [conn (d/create-conn)]
                  (doseq [ps (partition-all (:batch opts 1) (:people opts))]
                    (d/transact! conn ps))))
    :duration 5000
    :matrix   test-matrix-transact
    :setup-fn test-setup-people))

(defn sort-merge-join [db eis a]
  (let [min-e nil ;;(first eis)
        max-e nil ;;(nth eis (dec (count eis)))
        datoms (btset/slice (:aevt db) (d/Datom. min-e a nil nil nil)
                                       (d/Datom. max-e a nil nil nil))]
    (loop [_res []
           _ds  datoms
           _es  eis]
      (let [_fd (first _ds)
            _fe (first _es)]
        (if (and _fd _fe)
          (condp == (compare (:e _fd) _fe)
            0 (recur (conj _res [(:e _fd) (:v _fd)]) (next _ds) (next _es))
            -1 (recur _res (next _ds) _es)
            1  (recur _res _ds (next _es))
            )
          _res)))))

;; (defn hash-join [db eis a]
;;   (let [min-e (first eis)
;;         max-e (nth eis (dec (count eis)))
;;         datoms (btset/slice (:ae db) (d/Datom. min-e a nil nil nil)
;;                                      (d/Datom. max-e a nil nil nil))
;;         hashmap (reduce (fn [m d] (assoc m (.-e d) (.-v d))) {} datoms)]
;;     (set (map #(vector % (hashmap %)) eis))))


(def test-matrix-q [
  :test   ["q"]
  :method { 
;;     "q-scan"        (fn [opts] (d/q (concat '[:find ?e :where]
;;                                              (get-in opts [:lookup :q]))
;;                                     (:db opts)))
;;     "q-scan-join"   (fn [opts] (d/q (concat '[:find ?e ?ln :where]
;;                                              (get-in opts [:lookup :q])
;;                                             '[[?e :last-name ?ln]])
;;                                       (:db opts)))
;;     "q-scan-2joins" (fn [opts] (d/q (concat '[:find ?e ?ln ?s :where]
;;                                              (get-in opts [:lookup :q])
;;                                             '[[?e :last-name ?ln]
;;                                               [?e :salary ?s]])
;;                                       (:db opts)))

     "hash-join"        (fn [opts] (dq/search (:db opts)))
           
     "sort-join"        (fn [opts] (let [es (->> (d/-search (:db opts) [nil :name "Ivan"])
                                       (mapv :e))]
                           (sort-merge-join (:db opts) es :age)))
                                 
                                 
;;     "filter"        (fn [opts] (->> (:people opts)
;;                                     (filterv (get-in opts [:lookup :filter]))))
;;     "filter-set"    (fn [opts] (->> (:people opts)
;;                                     (filter (get-in opts [:lookup :filter]))
;;                                     (map (juxt :db/id :last-name :salary))
;;                                     set))
;;     "old-q"        (fn [opts] (d/q '[:find ?e ?v
;;                                      :where [?e :name "Ivan"]
;;                                             [?e :age ?v]]
;;                                   (:db opts)))


  }

  :lookup {
    "name"         {:q      '[[?e :name "Ivan"]]
                    :filter #(= (:name %) "Ivan") }

;;     "name+age"     {:q      '[[?e :name "Ivan"]
;;                               [?e :age 5]]
;;                     :filter #(and (= (:name %) "Ivan")
;;                               (= (:age %) 5))}

;;     "name+age+sex" {:q      '[[?e :name "Ivan"]
;;                               [?e :age 5]
;;                               [?e :sex :male]]
;;                     :filter #(and (= (:name %) "Ivan")
;;                                   (= (:age %) 5)
;;                                   (= (:sex %) :male)) }
  }
                    
  :size   [2000] ;; [100 500 2000 20000]
])


(defn test-setup-db [opts]
  (let [db (reduce #(d/with %1 [%2]) (d/empty-db) (:people opts))]
    (assoc opts :db db)))

#_(def people-db (:db (test-setup-db (test-setup-people {:size 10000}))))
#_(dq/search people-db)

#_(let [es (->> (d/-search people-db [nil :name "Ivan"])
                                    (mapv :e))]
            (sort-merge-join people-db es :age))

(defn ^:export perftest-q []
  (perf/suite (fn [opts] ((:method opts) opts))
    :duration 1000
    :matrix   test-matrix-q
    :setup-fn (comp test-setup-db test-setup-people)))

;; (perftest-q)

;; (def db (:db (test-setup-db (test-setup-people {:size 1000}))))

;; (let [es (->> (d/-search db [nil :name "Ivan"])
;;              (mapv :e))]
;;   (vec (hash-join db es :last-name)))

;; (vec (d/seek-datoms db :avet :age 8.5))

(defrecord X [a b c d])

(defn ^:export perftest-get-in []
  (perf/suite (fn [opts] ((:method opts) (:obj opts) (:path opts)))
    :duration 1000
    :matrix   [:obj    { "vec-vec" [[1 2 3 4] (X. 5 6 7 8) [9 10]] }
               :path   [[0 2] [1 :b]]
               :method { "get-in"   (fn [x p] (get-in x p))
                         "nth-get"  (fn [x [p1 p2]] (get (nth x p1) p2)) }]))


(defn ^:export perftest-getters []
  (perf/suite (fn [opts] ((:method opts) (X. 1 2 3 4)))
    :duration 1000
    :matrix   [:method { "prop"    (fn [x] (+ (.-a x) (.-b x) (.-c x) (.-d x)))
                         "keyword" (fn [x] (+ (:a x) (:b x) (:c x) (:d x)))
                         "aget"    (fn [x] (+ (aget x "a") (aget x "b") (aget x "c") (aget x "d"))) }]))


(defn ^:export perftest-array-getters []
  (perf/suite (fn [opts] ((:method opts) #js [1 2 3 4 5 6] [1 2 3 4 5 6]))
    :duration 1000
    :matrix   [:method { "aget a"  (fn [x _] (+ (aget x 0) (aget x 1) (aget x 2) (aget x 3)))
                         "nth  a"  (fn [x _] (+ (nth x 0) (nth x 1) (nth x 2) (nth x 3)))
                         "get  v"  (fn [_ x] (+ (get x 0) (get x 1) (get x 2) (get x 3)))
                         "nth  v"  (fn [_ x] (+ (nth x 0) (nth x 1) (nth x 2) (nth x 3)))}]))
