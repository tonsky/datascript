(ns datascript.query-v3
  (:require
    [datascript :as d]
    [datascript.query-v3 :as q]
     datascript.perf))

(comment

(defn rand-entity []
  {:name (rand-nth ["ivan" "oleg" "petr" "igor"])
   :age  (rand-int 10)})

(defn random-man []
  {:name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex       (rand-nth [:male :female])
   :age       (rand-int 10)
   :salary    (rand-int 100000)})

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
  (let [entities [{:name "Ivan" :age 1}
                  {:name "Ivan" :age 2}
                  {:name "Oleg" :age 1}
                  {:name "Oleg" :age 2}]
        db       (d/db-with (d/empty-db) entities)
        result   (q/q '[:find ?e ?a ?e2 ?a2
                        :in $
                        :where [?e  :name "Ivan"]
                               [?e2 :name "Oleg"]
                               [?e  :age ?a]
                               [?e2 :age ?a2]
                               [?e  :age ?a2]]
                      db)]
    result))



(require '[datascript.query-v3 :as q] :reload-all)

(defn bench [name q & args]
  (println "\n---\n")
  (let [old (perf/minibench (str "OLD " name) (apply d/q q args))
        new (perf/minibench (str "NEW " name) (apply q/q q args))]
    (= old new)))

(def db (d/db-with (d/empty-db) (repeatedly 1000 random-man)))

(require 'datascript.perf :reload-all)
(require '[datascript.query-v3 :as q] :reload)

(bench "q2 const"
        '[:find ?e ?e2 ?a ?a2
         :where [?e :name "Ivan"]
                [?e2 :name "Oleg"]
                [?e :age ?a]
                [?e2 :age ?a2]
                [?e :age ?a2]
          
;;                 [?e :sex ?s]
;;                 [?e :salary ?w]
;;                 [?e :last-name ?ln]
          ]
       db)

(bench "q2 const in"
       '[:find ?e
         :in $ ?n
         :where [?e :name "Ivan"]
                [?e :age ?n]]
       db 1)

)
