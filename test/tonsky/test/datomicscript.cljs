(ns tonsky.test.datomicscript
  (:require-macros
    [cemerick.cljs.test :refer (is deftest with-test run-tests testing test-var)])
  (:require
    [cemerick.cljs.test :as t]
    [tonsky.datomicscript :as d]))
(enable-console-print!)

(deftest test-transact
  (let [db  (-> (d/create-database {:aka { :cardinality :many }})
              (d/transact [[:add 1 :name "Ivan"]])
              (d/transact [[:add 1 :name "Petr"]])
              (d/transact [[:add 1 :aka  "Devil"]])
              (d/transact [[:add 1 :aka  "Tupen"]]))]
    
    (is (= (d/q '{:find [?v] :where [[1 :name ?v]]} db)
           #{["Petr"]}))
    (is (= (d/q '{:find [?v] :where [[1 :aka ?v]]} db)
           #{["Devil"] ["Tupen"]}))
    
    (testing "Retract"
      (let [db  (-> db
                  (d/transact [[:retract 1 :name "Petr"]])
                  (d/transact [[:retract 1 :aka  "Devil"]]))]

        (is (= (d/q '{:find [?v] :where [[1 :name ?v]]} db)
               #{}))
        (is (= (d/q '{:find [?v] :where [[1 :aka ?v]]} db)
               #{["Tupen"]}))))
    
    (testing "Cannot retract what's not there"
      (let [db  (-> db
                    (d/transact [[:retract 1 :name "Ivan"]]))]
        (is (= (d/q '{:find [?v] :where [[1 :name ?v]]} db)
               #{["Petr"]}))))
    ))

(deftest test-explode
  (let [db (-> (d/create-database)
               (d/transact [{:db/id 1
                             :name  "Ivan"
                             :age   16}]))]
    (is (= (d/q '{:find [?n ?a] :where [[1 :name ?n]
                                        [1 :age ?a]]} db)
           #{["Ivan" 16]}))))

;; TODO vectors in explode

;; (deftest test-explode-many
;;   (let [db (-> (d/create-database {:aka { :cardinality :many }})
;;                (d/transact [{:db/id 1
;;                              :name  "Ivan"
;;                              :aka   ["Devil" "Tupen"]}]))]
;;     (is (= (d/q '{:find [?v] :where [[1 :name ?v]]} db)
;;            #{["Ivan"]}))
;;     (is (= (d/q '{:find [?v] :where [[1 :aka ?v]]} db)
;;            #{["Devil"] ["Tupen"]}))))

(deftest test-joins
  (let [db (-> (d/create-database)
               (d/transact [ { :db/id 1
                               :name  "Ivan"
                               :age   15 }
                             { :db/id 2
                               :name  "Petr"
                               :age   37 }
                             { :db/id 3
                               :name  "Ivan"
                               :age   37 }]))]
    (is (= (d/q '{:find [?e ?v]
                  :where [[?e :name "Ivan"]
                          [?e :age ?v]]} db)
           #{[1 15] [3 37]}))
    (is (= (d/q '{:find [?e1 ?e2]
                  :where [[?e1 :name ?n]
                          [?e2 :name ?n]]} db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '{:find [?e ?e2 ?n]
                  :where [[?e :name "Ivan"]
                          [?e :age ?a]
                          [?e2 :age ?a]
                          [?e2 :name ?n]]} db)
           #{[1 1 "Ivan"] [3 3 "Ivan"] [3 2 "Petr"]}))))

(deftest test-q-many
  (let [db (-> (d/create-database {:aka {:cardinality :many}})
               (d/transact [ [:add 1 :name "Ivan"]
                             [:add 1 :aka  "ivolga"]
                             [:add 1 :aka  "pi"]
                             [:add 2 :name "Petr"]
                             [:add 2 :aka  "porosenok"]
                             [:add 2 :aka  "pi"] ]))]
    (is (= (d/q '{:find [?n1 ?n2]
                  :where [[?e1 :aka ?x]
                          [?e2 :aka ?x]
                          [?e1 :name ?n1]
                          [?e2 :name ?n2]]} db)
           #{["Ivan" "Ivan"] ["Petr" "Petr"] ["Ivan" "Petr"] ["Petr" "Ivan"]}))))
                              

(t/test-ns 'tonsky.test.datomicscript)


;; (defn random-man []
;;   (let [id (rand-int 1000000)]
;;     {:db/id id
;;      :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
;;      :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
;;      :sex       (rand-nth [:male :female])
;;      :age       (rand-int 90)}))

;; (defn eid->entity [db eid]
;;   (let [datoms (search-by-one db :e eid)]
;;     (reduce #(assoc %1 (.-a %2) (.-v %2)) {:db/id eid} datoms)))

;; (def big-db (reduce transact (create-database nil) (repeatedly 2000 #(vector random-man))))

;; (count (search-by-two big-db :a :name :v "Ivan"))
;; (eid->entity big-db (:e (first (search-by-two big-db :a :name :v "Ivan"))))

;; (q '{:find [?e ?a ?s]
;;      :where [[?e :name "Ivan"]
;;              [?e :age ?a]
;;              [?e :sex ?s]]}
;;    big-db)
