(ns datascript-bench.datascript
  (:require
    [clojure.string :as str]
    [datascript.core :as d]
    [datascript-bench.core :as core]
    [datascript.query-v3 :as q3]))


#?(:cljs
   (enable-console-print!))


(def schema
  { :follows { :db/valueType   :db.type/ref
               :db/cardinality :db.cardinality/many } })


(defn- wide-db
  ([depth width] (d/db-with (d/empty-db schema) (wide-db 1 depth width)))
  ([id depth width]
    (if (pos? depth)
      (let [children (map #(+ (* id width) %) (range width))]
        (concat
          (map #(array-map
                  :db/add  id
                  :name    "Ivan"
                  :follows %) children)
          (mapcat #(wide-db % (dec depth) width) children)))
      [{:db/id id :name "Ivan"}])))


(defn- long-db [depth width]
  (d/db-with (d/empty-db schema)
    (apply concat
      (for [x (range width)
            y (range depth)
            :let [from (+ (* x (inc depth)) y)
                  to   (+ (* x (inc depth)) y 1)]]
        [{:db/id     from
            :name    "Ivan"
            :follows to}
           {:db/id   to
            :name    "Ivan"}]))))


(def db100k
  (d/db-with (d/empty-db) core/people20k))


(defn ^:export add-1 []
  (core/bench
    (reduce
      (fn [db p]
        (-> db
          (d/db-with [[:db/add (:db/id p) :name      (:name p)]])
          (d/db-with [[:db/add (:db/id p) :last-name (:last-name p)]])
          (d/db-with [[:db/add (:db/id p) :sex       (:sex p)]])
          (d/db-with [[:db/add (:db/id p) :age       (:age p)]])
          (d/db-with [[:db/add (:db/id p) :salary    (:salary p)]])))
      (d/empty-db)
      core/people20k)))


(defn ^:export add-5 []
  (core/bench
    (reduce (fn [db p] (d/db-with db [p])) (d/empty-db) core/people20k)))


(defn ^:export add-all []
  (core/bench
    (d/db-with (d/empty-db) core/people20k)))


(defn ^:export init []
  (let [datoms (into []
                 (for [p core/people20k
                       :let [id (#?(:clj Integer/parseInt :cljs js/parseInt) (:db/id p))]
                       [k v] p
                       :when (not= k :db/id)]
                   (d/datom id k v)))]
    (core/bench
      (d/init-db datoms))))


(defn ^:export retract-5 []
  (let [db   (d/db-with (d/empty-db) core/people20k)
        eids (->> (d/datoms db :aevt :name) (map :e) (shuffle))]
    (core/bench
      (reduce (fn [db eid] (d/db-with db [[:db.fn/retractEntity eid]])) db eids))))


(defn ^:export q1 []
  (core/bench
    (d/q '[:find ?e
           :where [?e :name "Ivan"]]
      db100k)))


(defn ^:export q2 []
  (core/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]]
      db100k)))


(defn ^:export q3 []
  (core/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e :sex :male]]
      db100k)))


(defn ^:export q4 []
  (core/bench
    (d/q '[:find ?e ?l ?a
           :where [?e :name "Ivan"]
                  [?e :last-name ?l]
                  [?e :age ?a]
                  [?e :sex :male]]
      db100k)))


(defn ^:export qpred1 []
  (core/bench
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      db100k)))


(defn ^:export qpred2 []
  (core/bench
    (d/q '[:find ?e ?s
           :in   $ ?min_s
           :where [?e :salary ?s]
                  [(> ?s ?min_s)]]
      db100k 50000)))


(defn ^:export bench-rules []
  (doseq [[id db] [["wide 3×3" (wide-db 3 3)]
                   ["wide 5×3" (wide-db 5 3)]
                   ["wide 7×3" (wide-db 7 3)]
                   ["wide 4×6" (wide-db 4 6)]
                   ["long 10×3" (long-db 10 3)]
                   ["long 30×3" (long-db 30 3)]
                   ["long 30×5" (long-db 30 5)]]]
    (core/bench {:test "rules" :form id}
      (d/q '[:find ?e ?e2
             :in   $ %
             :where (follows ?e ?e2)]
           db
           '[[(follows ?x ?y)
              [?x :follows ?y]]
             [(follows ?x ?y)
              [?x :follows ?t]
              (follows ?t ?y)]]))))


#?(:clj
   (defn ^:export -main [& names]
     (doseq [n names]
       (if-some [benchmark (ns-resolve 'datascript-bench.datascript (symbol n))]
         (let [perf (benchmark)]
           (print (core/round perf) "\t")
           (flush))
         (do
           (print "---" "\t")
           (flush))))
     (println)))
