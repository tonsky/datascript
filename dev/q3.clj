(ns datascript.query-v3
  (:require
    [datascript.core :as d]
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

(require 'datascript.perf :reload-all)
(require '[datascript.query-v3 :as q] :reload)

(perf/with-debug
  (let [entities [{:db/id 1 :name "Ivan" :age 10}
                  {:db/id 2 :name "Ivan" :age 20}
                  {:db/id 3 :name "Oleg" :age 10}
                  {:db/id 4 :name "Oleg" :age 20}
                  {:db/id 5 :name "Ivan" :age 10}
                  {:db/id 6 :name "Ivan" :age 20}
                  ]
        db       (d/db-with (d/empty-db) entities)
        result   (q/q '[:find ?e ?a
                        :where [?e :age ?a]
                               (not [?e :name "Ivan"])]
                      db)]
    result))



(let [entities [{:db/id 1 :name "Ivan" :age 10}
                {:db/id 2 :name "Ivan" :age 20}
                {:db/id 3 :name "Oleg" :age 10}
                {:db/id 4 :name "Oleg" :age 20}
                {:db/id 5 :name "Ivan" :age 10}
                {:db/id 6 :name "Ivan" :age 20}]
      db       (d/db-with (d/empty-db) entities)]
  (perf/with-debug
    (q/q '[:find ?e ?e2
           :where [?e  :name "Ivan"]
                  [?e2 :name "Ivan"]
           (not [?e  :age 10]
                [?e2 :age 20])]
         db)))
                  

(require '[datascript.query-v3 :as q] :reload-all)

(defn bench [name q & args]
  (println "\n---\n")
  (let [old (perf/minibench (str "OLD " name) (apply d/q q args))
        new (perf/minibench (str "NEW " name) (apply q/q q args))]
    (= old new)))

(def db (d/db-with (d/empty-db) (repeatedly 2000 random-man)))

(require 'datascript.perf :reload-all)
(require '[datascript.query-v3 :as q] :reload)

;; (require 'datascript.test.query-not :reload)
;; (clojure.test/test-ns 'datascript.test.query-not)

(bench "pred"
  '[:find ?e ?a
    :where [?e :age ?a]
           [(> ?a 5)]]
  db)

(bench "join + pred"
  '[:find ?e ?e2
    :where [?e  :name "Ivan"]
           [?e2 :name "Ivan"]
           [(not= ?e ?e2)]]
  db)


(bench "prod + join"
        '[:find ?e ?e2 ?a ?a2
         :where [?e :name "Ivan"]
                [?e2 :name "Oleg"]
                [?e :age ?a]
                [?e2 :age ?a2]
                [?e :age ?a2]]
       db)

(bench "q5"
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
