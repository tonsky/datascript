(ns datascript.test.perf
  (:require
    [datascript.btset :as btset]
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.perf :as perf]
    [goog.array :as garray]))

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
                            :batch  [1]])

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

(defn- setup-datoms [opts]
  (let [people (repeatedly (:size opts) random-man)
        people (map #(assoc %1 :db/id %2) people (range))
        datoms (mapcat (fn [person]
                         (map (fn [[k v]]
                                (dc/Datom. (:db/id person) k v (+ d/tx0 (rand-int 1000)) true))
                              (dissoc person :db/id)))
                       people)]
    (assoc opts :datoms (vec datoms))))

(defn ^:export perftest-from-reader []
  (perf/suite (fn [opts]
                ((:fn opts) (:datoms opts)))
    :duration 5000
    :matrix   [ :fn   {;; "old" d/db-from-reader
                       "new" d/init-db}
                :size [100 500 2000 5000 20000] ]
    :setup-fn setup-datoms))
      

(defn sort-merge-join [db eis a]
  (let [min-e nil ;;(first eis)
        max-e nil ;;(nth eis (dec (count eis)))
        datoms (btset/slice (:aevt db) (dc/Datom. min-e a nil nil nil)
                                       (dc/Datom. max-e a nil nil nil))]
    (loop [_res []
           _ds  datoms
           _es  eis]
      (let [_fd (first _ds)
            _fe (first _es)]
        (if (and _fd _fe)
          (case (compare (.-e _fd) _fe)
            0 (recur (conj _res [(.-e _fd) (.-v _fd)]) (next _ds) (next _es))
           -1 (recur _res (next _ds) _es)
            1 (recur _res _ds (next _es)))
          _res)))))

(defn hash-join [db eis a]
;;  (.time js/console "hash-join")
;;   (.log js/console (str "eis: " (count eis)))
  (let [min-e nil ;; (first eis)
        max-e nil ;; (nth eis (dec (count eis)))
        datoms (btset/slice (:aevt db) (dc/Datom. min-e a nil nil nil)
                                       (dc/Datom. max-e a nil nil nil))
;;        _ (.time js/console "hash")
        hashmap (reduce (fn [m d] (assoc m (.-e d) (conj (get m (.-e d) '()) (.-v d)))) {} datoms)
;;        _ (.timeEnd js/console "hash")
;;         _       (.log js/console (str "hashmap: " (count hashmap)))
;;        _ (.time js/console "join")
        res     (reduce (fn [acc e] (if-let [vs (get hashmap e)]
                                      (reduce (fn [acc v]
                                                (conj acc #js [e v]))
                                                acc vs)
                                      acc))
                                      [] eis)
;;        _ (.timeEnd js/console "join")
        ]
;;    (.timeEnd js/console "hash-join")
    (into #{} (map vec res))))


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
     "d/q1"              (fn [opts] (d/q '[ :find  ?e ?a
                                           :where [?e :name "Ivan"]]
                                         (:db opts)))


     "d/q2"              (fn [opts] (d/q '[ :find  ?e ?a
                                           :where [?e :name "Ivan"]
                                                  [?e :age ?a] ]
                                         (:db opts)))

     "d/q3"              (fn [opts] (d/q '[ :find  ?e ?a
                                           :where [?e :name "Ivan"]
                                                  [?e :age ?a]
                                                  [?e :sex :male] ]
                                         (:db opts)))

     ; "hash-join"        (fn [opts] (let [es (->> (dc/-search (:db opts) [nil :name "Ivan"])
     ;                                             (mapv :e))]
     ;                       (hash-join (:db opts) es :age)))

     ; "sort-join"        (fn [opts] (let [es (->> (dc/-search (:db opts) [nil :name "Ivan"])
     ;                                             (mapv :e))]
     ;                       (sort-merge-join (:db opts) es :age)))


                                 
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

  ; :lookup {
    ; "name"         {:q      '[[?e :name "Ivan"]]
    ;                 :filter #(= (:name %) "Ivan") }

;;     "name+age"     {:q      '[[?e :name "Ivan"]
;;                               [?e :age 5]]
;;                     :filter #(and (= (:name %) "Ivan")
;;                               (= (:age %) 5))}

    ; "name+age+sex" {:q      '[[?e :name "Ivan"]
    ;                           [?e :age 5]
    ;                           [?e :sex :male]]
    ;                 :filter #(and (= (:name %) "Ivan")
    ;                               (= (:age %) 5)
    ;                               (= (:sex %) :male)) }
  ; }
                    
  :size [500 2000] ;; [100 500 2000 20000]
])


(defn test-setup-db [opts]
  (let [db (reduce #(d/db-with %1 [%2]) (d/empty-db) (:people opts))]
    (assoc opts :db db)))

(defn ^:export perftest-q []
  (perf/suite (fn [opts] ((:method opts) opts))
    :duration 1000
    :matrix   test-matrix-q
    :setup-fn (comp test-setup-db test-setup-people)))

(defn- gen-wide-db [id depth width]
  (if (pos? depth)
    (let [children (map #(+ (* id width) %) (range width))]
      (concat
        (map #(vector :db/add id :follows %) children)
        (mapcat #(gen-wide-db % (dec depth) width) children)))
    []))

(defn- gen-long-db [depth width]
  (for [x (range width)
        y (range depth)]
    [:db/add (+ (* x (inc depth)) y) :follows (+ (* x (inc depth)) y 1)]))

(defn ^:export perftest-rules []
  (perf/suite (fn [opts]
                (d/q
                   '[:find ?e ?e2
                     :in $ %
                     :where (follows ?e ?e2)]
                  (:db opts)
                  '[[(follows ?x ?y)
                     [?x :follows ?y]]
                    [(follows ?x ?y)
                     [?x :follows ?t]
                     (follows ?t ?y)]]))
    :duration 1000
    :matrix   ["form" [
                       [:wide 3 3]
                       [:wide 5 3]
                       [:wide 7 3]
                       [:wide 4 6]
                       [:long 10 3]
                       [:long 5 5]
                       [:long 30 3]
                       [:long 30 5]
                       ]]
    :setup-fn (fn [opts]
                (let [[form depth width] (get opts "form")
                      datoms (case form
                               :wide (gen-wide-db 1 depth width)
                               :long (gen-long-db depth width))
                      db (reduce #(d/with %1 [%2]) (d/empty-db) datoms)]
                  (assoc opts :db db)))))

(def now datascript.perf/now)

(defn ^:export perftest-db-hash []
  (let [datoms (gen-long-db 30 10)]
    (loop [time  0
           iters 0]
      (if (< iters 100)
        (let [db (reduce #(d/with %1 [%2]) (d/empty-db) datoms)
              t0 (now)]
          (hash db)
          (recur (+ time (- (now) t0)) (inc iters)))
        (let [dt (/ (* 1000 time) iters)]
          (println "perftest-db-hash:" dt "ms")
          dt)))))


(defn ^:export perftest-db-equiv []
  (let [datoms (gen-long-db 30 10)]
    (loop [time  0
           iters 0]
      (if (< iters 100)
        (let [db1    (reduce #(d/with %1 [%2]) (d/empty-db) datoms)
              db2    (reduce #(d/with %1 [%2]) (d/empty-db) datoms)
              t0     (now)]
          (= db1 db2)
          (recur (+ time (- (now) t0)) (inc iters)))
        (let [dt (/ (* 1000 time) iters)]
          (println "perftest-db-equiv" dt "ms")
          dt)))))

(defn ^:export perftest-db-equiv []
  (let [datoms (gen-long-db 30 10)]
    (loop [time  0
           iters 0]
      (if (< iters 100)
        (let [db1    (reduce #(d/with %1 [%2]) (d/empty-db) datoms)
              db2    (reduce #(d/with %1 [%2]) (d/empty-db) datoms)
              t0     (now)]
          (= db1 db2)
          (recur (+ time (- (now) t0)) (inc iters)))
        (let [dt (/ (* 1000 time) iters)]
          (println "perftest-db-equiv" dt "ms")
          dt)))))

(def letters  (seq "abcdefghijklmnopqrstuvwxyz"))
(def alphabet (concat letters (seq "-------_1234567890")))
(defn rand-str [n]
  (apply str (rand-nth letters) (repeatedly (rand-int n) #(rand-nth alphabet))))
(defn rand-keyword []
  (if (> (rand) 0.5)
    (keyword (rand-str 10) (rand-str 20))
    (keyword (rand-str 20))))

(defn compare-keywords-1 [a b]
  (cond
   (= a b) 0
   (and (not (.-ns a)) (.-ns b)) -1
   (.-ns a) (if-not (.-ns b)
              1
              (let [nsc (garray/defaultCompare (.-ns a) (.-ns b))]
                (if (zero? nsc)
                  (garray/defaultCompare (.-name a) (.-name b))
                  nsc)))
   :default (garray/defaultCompare (.-name a) (.-name b))))

(defn compare-keywords-quick [a b]
  (cond
   (identical? (.-fqn a) (.-fqn b)) 0
   (and (not (.-ns a)) (.-ns b)) -1
   (.-ns a) (if-not (.-ns b)
              1
              (let [nsc (garray/defaultCompare (.-ns a) (.-ns b))]
                (if (zero? nsc)
                  (garray/defaultCompare (.-name a) (.-name b))
                  nsc)))
   :default (garray/defaultCompare (.-name a) (.-name b))))

(defn ^:export perftest-keywords-compare []
  (perf/suite (fn [opts] (.sort (:set opts) (:fn opts)))
    :duration 1000
    :matrix   [:fn {"compare-symbols" compare-symbols
                    "compare-keywords-1" compare-keywords-1
                    "compare-keywords-quick" compare-keywords-quick
                    }
               :size [5000]]
    :setup-fn (fn [opts]
                (let [seed (vec (repeatedly 50 rand-keyword))
                      set  (repeatedly (:size opts) #(rand-nth seed))]
                  (assoc opts :set (into-array set))))))
