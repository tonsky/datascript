(ns datascript.query-v3
  (:require
    [datascript :as d]
    [datascript.query-v3 :as q]
     datascript.perf))

(comment

(perf/minibench "q coll"
  (q/q '[:find ?a
       :in $1 $2 ?n
       :where [$1 ?a ?n ?b]
              [$2 ?n ?a]]
     (repeatedly 100 (fn [] [(rand-nth [:a :b :c :d]) (rand-int 10) (rand-nth [:x :y :z])]))
     (repeatedly 10 (fn [] [(rand-int 10) (rand-nth [:a :b :c :d]) ]))
     1))

(require '[datascript.query-v3 :as q] :reload)

(binding [datascript.perf/debug? true]
  (let [entities (repeatedly 5 rand-entity)
  ;;       _        (println entities)
        db       (d/db-with (d/empty-db) entities)
        result   (q/q '[:find ?e ?n ?e2 ?a
                        :in $
                        :where [?e :name ?n]
                               #_[?e2 :name ?n]
                               [?e2 :age ?a]]
                      db)]
    #_[entities result]
    result))


(defn rand-entity []
  {:name (rand-nth ["ivan" "oleg" "petr" "igor"])
   :age  (rand-int 10)})

(defn random-man []
  {:name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex       (rand-nth [:male :female])
   :age       (rand-int 10)
   :salary    (rand-int 100000)})

(require '[datascript.query-v3 :as q] :reload-all)

(defn bench [name q & args]
  (println "\n---\n")
  (perf/minibench (str "OLD " name) (apply d/q q args))
  (perf/minibench (str "NEW " name) (apply q/q q args))
  nil)

(defonce db (d/db-with (d/empty-db) (repeatedly 100000 random-man)))

(require 'datascript.perf :reload-all)
(require '[datascript.query-v3 :as q] :reload)

(bench "q2 const"
        '[:find ?e ?a ?s ?w ?ln
         :where [?e :name "Ivan"]
                [?e :age ?a]
                [?e :sex ?s]
                [?e :salary ?w]
                [?e :last-name ?ln]]
       db)

(bench "q2 const in"
       '[:find ?e
         :in $ ?n
         :where [?e :name "Ivan"]
                [?e :age ?n]]
       db 1)
)
