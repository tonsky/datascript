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
               (d/transact [ { :db/id 1, :name  "Ivan", :age   15 }
                             { :db/id 2, :name  "Petr", :age   37 }
                             { :db/id 3, :name  "Ivan", :age   37 }]))]
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
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))))

(deftest test-q-coll
  (let [db [ [1 :name "Ivan"]
             [1 :age  19]
             [1 :aka  "dragon_killer_94"]
             [1 :aka  "-=autobot=-"] ] ]
    (is (= (d/q '{ :find [?n ?a]
                   :where [[?e :aka "dragon_killer_94"]
                           [?e :name ?n]
                           [?e :age  ?a]]} db)
           #{["Ivan" 19]})))
  
  (testing "Query over long tuples"
    (let [db [ [1 :name "Ivan" 945 :add]
               [1 :age  39     999 :retract]] ]
      (is (= (d/q '{ :find [?e ?v]
                     :where [[?e :name ?v]]} db)
             #{[1 "Ivan"]}))
      (is (= (d/q '{ :find [?e ?a ?v ?t]
                     :where [[?e ?a ?v ?t :retract]]} db)
             #{[1 :age 39 999]})))))

(deftest test-q-in
  (let [db (-> (d/create-database)
               (d/transact [ { :db/id 1, :name  "Ivan", :age   15 }
                             { :db/id 2, :name  "Petr", :age   37 }
                             { :db/id 3, :name  "Ivan", :age   37 }]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (d/q query db :age 37)
           #{[2] [3]}))
    
    (testing "Named DB"
      (is (= (d/q '{:find  [?a ?v]
                    :in    [$db ?e]
                    :where [[$db ?e ?a ?v]]} db 1)
             #{[:name "Ivan"]
               [:age 15]})))
    
    (testing "DB join with collection"
      (is (= (d/q '{:find  [?e ?email]
                    :in    [$ $b]
                    :where [[?e :name ?n]
                            [$b ?n ?email]]}
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))
    
    (testing "Relation binding"
      (is (= (d/q '{:find  [?e ?email]
                    :in    [$ [[?n ?email]]]
                    :where [[?e :name ?n]]}
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))
    
    (testing "Tuple binding"
      (is (= (d/q '{:find  [?e]
                    :in    [$ [?name ?age]]
                    :where [[?e :name ?name]
                            [?e :age ?age]]}
                  db ["Ivan" 37])
             #{[3]})))
    
    (testing "Collection binding"
      (is (= (d/q '{:find  [?attr ?value]
                    :in    [$ ?e [?attr ...]]
                    :where [[?e ?attr ?value]]}
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]})))
  )
  
  (testing "Query without DB"
    (is (= (d/q '{:find [?a ?b]
                  :in [?a ?b]}
                10 20)
           #{[10 20]}))))

(deftest test-nested-bindings
  (is (= (d/q '{:find [?k ?v]
                :in [[[?k ?v] ...]]
                :where [[(> ?v 1)]]}
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))
  
  (is (= (d/q '{:find [?k ?min ?max]
                :in [[[?k ?v] ...] ?minmax]
                :where [[(?minmax ?v) [?min ?max]]
                        [(> ?max ?min)]]}
              {:a [1 2 3 4],
               :b [5 6 7],
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))
  
  (is (= (d/q '{:find [?k ?x]
                :in [[[?k [?min ?max]] ...] ?range]
                :where [[(?range ?min ?max) [?x ...]]
                        [(even? ?x)]]}
              {:a [1 7], :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6] [:b 2]}))
  )


(deftest test-user-funs
  (let [db (-> (d/create-database)
               (d/transact [ { :db/id 1, :name  "Ivan",  :age   15 }
                             { :db/id 2, :name  "Petr",  :age   22 }
                             { :db/id 3, :name  "Slava", :age   37 }]))]
    (testing "Built-in predicate"
      (is (= (d/q '{:find  [?e1 ?e2]
                    :where [[?e1 :age ?a1]
                            [?e2 :age ?a2]
                            [(< ?a1 18 ?a2)]]} db)
             #{[1 2] [1 3]})))
    
    (testing "Passing predicate as source"
      (is (= (d/q '{:find  [?e]
                    :in [$ ?adult]
                    :where [[?e :age ?a]
                            [(?adult ?a)]]}
                  db
                  #(> % 18))
             #{[2] [3]})))
    
    (testing "Calling a function"
      (is (= (d/q '{:find  [?e1 ?e2 ?e3]
                    :where [[?e1 :age ?a1]
                            [?e2 :age ?a2]
                            [?e3 :age ?a3]
                            [(+ ?a1 ?a2) ?a12]
                            [(= ?a12 ?a3)]]}
                  db)
             #{[1 2 3] [2 1 3]})))
    ))

(t/test-ns 'tonsky.test.datomicscript)




;; Performance

(defn now [] (.getTime (js/Date.)))
(defn measure [f]
  (let [t0 (now)
        res (f)]
    (- (now) t0)))

(defn random-man []
  (let [id (rand-int 1000000)]
    {:db/id id
     :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
     :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
     :sex       (rand-nth [:male :female])
     :age       (rand-int 90)}))

;; (def big-db (reduce d/transact
;;               (d/create-database)
;;               (repeatedly 2000 #(vector (random-man)))))
;; (measure #(d/q '{:find [?e ?a ?s]
;;                  :where [[?e :name "Ivan"]
;;                          [?e :age ?a]
;;                          [?e :sex ?s]]}
;;            big-db))
