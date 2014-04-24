(ns test.datomicscript
  (:require-macros
    [cemerick.cljs.test :refer (is deftest with-test run-tests testing test-var)])
  (:require
    [cemerick.cljs.test :as t]
    [datomicscript :as d]))

(enable-console-print!)


(deftest test-with
  (let [db  (-> (d/empty-db {:aka { :cardinality :many }})
                (d/with [[:db/add 1 :name "Ivan"]])
                (d/with [[:db/add 1 :name "Petr"]])
                (d/with [[:db/add 1 :aka  "Devil"]])
                (d/with [[:db/add 1 :aka  "Tupen"]]))]
    
    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] db)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] db)
           #{["Devil"] ["Tupen"]}))
    
    (testing "Retract"
      (let [db  (-> db
                  (d/with [[:db/retract 1 :name "Petr"]])
                  (d/with [[:db/retract 1 :aka  "Devil"]]))]

        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{}))
        (is (= (d/q '[:find ?v
                      :where [1 :aka ?v]] db)
               #{["Tupen"]}))))
    
    (testing "Cannot retract what's not there"
      (let [db  (-> db
                    (d/with [[:db/retract 1 :name "Ivan"]]))]
        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{["Petr"]}))))))

(deftest test-retract-fns
  (let [db (-> (d/empty-db {:aka { :cardinality :many }})
               (d/with [ { :db/id 1, :name  "Ivan", :age 15, :aka ["X" "Y" "Z"] }
                         { :db/id 2, :name  "Petr", :age 37 } ]))]
    (let [db (d/with db [ [:db.fn/retractEntity 1] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))
    
    (let [db (d/with db [ [:db.fn/retractAttribute 1 :name] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))
    
    (let [db (d/with db [ [:db.fn/retractAttribute 1 :aka] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:name "Ivan"] [:age 15]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))))


(deftest test-transact!
  (let [conn (d/create-conn {:aka { :cardinality :many }})]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]])
    
    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))))


(deftest test-resolve-eid
  (let [conn (d/create-conn)
        t1   (d/transact! conn [[:db/add -1 :name "Ivan"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Petr"]
                                [:db/add -2 :age 22]])
        t2   (d/transact! conn [[:db/add -1 :name "Sergey"]
                                [:db/add -1 :age 30]])]
    (is (= (:tempids t1) { -1 1, -2 2 }))
    (is (= (:tempids t2) { -1 3 }))
    (is (= (d/q '[:find  ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                         [?e :age ?a]] @conn)
           #{[1 "Ivan" 19   (+ d/tx0 1)]
             [2 "Petr" 22   (+ d/tx0 1)]
             [3 "Sergey" 30 (+ d/tx0 2)]}))))

(deftest test-entity
  (let [conn (d/create-conn {:aka {:cardinality :many}})
        p1   {:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
        p2   {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
        t1   (d/transact! conn [p1 p2])]
    (is (= (d/entity @conn 1) p1))
    (is (= (d/entity @conn 2) p2))))

(deftest test-listen!
  (let [conn    (d/create-conn)
        reports (atom [])]
    (d/transact! conn [[:db/add -1 :name "Alex"]
                       [:db/add -2 :name "Boris"]])
    (d/listen! conn :test #(swap! reports conj %))
    (d/transact! conn [[:db/add -1 :name "Dima"]
                       [:db/add -1 :age 19]
                       [:db/add -2 :name "Evgeny"]])
    (d/transact! conn [[:db/add -1 :name "Fedor"]
                       [:db/add 1 :name "Alex2"]         ;; should update
                       [:db/retract 2 :name "Not Boris"] ;; should be skipped
                       [:db/retract 4 :name "Evgeny"]])
    (d/unlisten! conn :test)
    (d/transact! conn [[:db/add -1 :name "Geogry"]])
    (is (= (map (fn [report] (map #(into [] %) (:tx-data report))) @reports)
           [[[3 :name "Dima"   (+ d/tx0 2) true]
             [3 :age 19        (+ d/tx0 2) true]
             [4 :name "Evgeny" (+ d/tx0 2) true]]
            [[5 :name "Fedor"  (+ d/tx0 3) true]
             [1 :name "Alex"   (+ d/tx0 3) false] ;; update -> retract
             [1 :name "Alex2"  (+ d/tx0 3) true]  ;;         + add
             [4 :name "Evgeny" (+ d/tx0 3) false]]]))))


(deftest test-explode
  (let [conn (d/create-conn { :aka { :cardinality :many }
                              :also { :cardinality :many} })]
    (d/transact! conn [{:db/id -1
                        :name  "Ivan"
                        :age   16
                        :aka   ["Devil" "Tupen"]
                        :also  "ok"}])
    (is (= (d/q '[:find  ?n ?a
                  :where [1 :name ?n]
                         [1 :age ?a]] @conn)
           #{["Ivan" 16]}))
    (is (= (d/q '[:find  ?v
                  :where [1 :also ?v]] @conn)
           #{["ok"]}))
    (is (= (d/q '[:find  ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))))


(deftest test-joins
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan", :age   15 }
                         { :db/id 2, :name  "Petr", :age   37 }
                         { :db/id 3, :name  "Ivan", :age   37 }
                         { :db/id 4, :age 15 }]))]
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                         [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                         [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                         [?e :age ?a]
                         [?e2 :age ?a]
                         [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))))


(deftest test-q-many
  (let [db (-> (d/empty-db {:aka {:cardinality :many}})
               (d/with [ [:db/add 1 :name "Ivan"]
                         [:db/add 1 :aka  "ivolga"]
                         [:db/add 1 :aka  "pi"]
                         [:db/add 2 :name "Petr"]
                         [:db/add 2 :aka  "porosenok"]
                         [:db/add 2 :aka  "pi"] ]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where [?e1 :aka ?x]
                         [?e2 :aka ?x]
                         [?e1 :name ?n1]
                         [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))))


(deftest test-q-coll
  (let [db [ [1 :name "Ivan"]
             [1 :age  19]
             [1 :aka  "dragon_killer_94"]
             [1 :aka  "-=autobot=-"] ] ]
    (is (= (d/q '[ :find  ?n ?a
                   :where [?e :aka "dragon_killer_94"]
                          [?e :name ?n]
                          [?e :age  ?a]] db)
           #{["Ivan" 19]})))
  
  (testing "Query over long tuples"
    (let [db [ [1 :name "Ivan" 945 :db/add]
               [1 :age  39     999 :db/retract]] ]
      (is (= (d/q '[ :find  ?e ?v
                     :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (d/q '[ :find  ?e ?a ?v ?t
                     :where [?e ?a ?v ?t :db/retract]] db)
             #{[1 :age 39 999]})))))


(deftest test-q-in
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan", :age   15 }
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
      (is (= (d/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))
    
    (testing "DB join with collection"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                           [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))
    
    (testing "Relation binding"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))
    
    (testing "Tuple binding"
      (is (= (d/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                           [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))
    
    (testing "Collection binding"
      (is (= (d/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]}))))
  
  (testing "Query without DB"
    (is (= (d/q '[:find ?a ?b
                  :in   ?a ?b]
                10 20)
           #{[10 20]}))))


(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))
  
  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                       [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))
  
  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                       [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))


(deftest test-user-funs
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan",  :age   15 }
                         { :db/id 2, :name  "Petr",  :age   22 }
                         { :db/id 3, :name  "Slava", :age   37 }]))]
    (testing "Built-in predicate"
      (is (= (d/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [(< ?a1 18 ?a2)]] db)
             #{[1 2] [1 3]})))
    
    (testing "Passing predicate as source"
      (is (= (d/q '[:find  ?e
                    :in    $ ?adult
                    :where [?e :age ?a]
                           [(?adult ?a)]]
                  db
                  #(> % 18))
             #{[2] [3]})))
    
    (testing "Calling a function"
      (is (= (d/q '[:find  ?e1 ?e2 ?e3
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [?e3 :age ?a3]
                           [(+ ?a1 ?a2) ?a12]
                           [(= ?a12 ?a3)]]
                  db)
             #{[1 2 3] [2 1 3]})))))


(deftest test-rules
  (let [db [                  [5 :follow 3]
            [1 :follow 2] [2 :follow 3] [3 :follow 4] [4 :follow 6]
                          [2         :follow           4]]]
    (is (= (d/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
               '[[(follow ?x ?y)
                  [?x :follow ?y]]])
           #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))
    
    (testing "Rule with branches"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e2 ?e1)
                    [?e2 :follow ?e1]]
                   [(follow ?e2 ?e1)
                    [?e2 :follow ?t]
                    [?t  :follow ?e1]]])
             #{[2] [3] [4]})))
    
    (testing "Recursive rule"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e1 ?e2)
                    [?e1 :follow ?e2]]
                   [(follow ?e1 ?e2)
                    [?e1 :follow ?t]
                    (follow ?t ?e2)]])
             #{[2] [3] [4] [6]})))
    
    (testing "Mutually recursive rules"
      (is (= (d/q '[:find  ?e1 ?e2
                    :in    $ %
                    :where (f1 ?e1 ?e2)]
                  [[0 :f1 1]
                   [1 :f2 2]
                   [2 :f1 3]
                   [3 :f2 4]
                   [4 :f1 5]
                   [5 :f2 6]]
                 '[[(f1 ?e1 ?e2)
                    [?e1 :f1 ?e2]]
                   [(f1 ?e1 ?e2)
                    [?t :f1 ?e2]
                    (f2 ?e1 ?t)]
                   [(f2 ?e1 ?e2)
                    [?e1 :f2 ?e2]]
                   [(f2 ?e1 ?e2)
                    [?t :f2 ?e2]
                    (f1 ?e1 ?t)]])
            #{[0 1] [0 3] [0 5]
              [1 3] [1 5]
              [2 3] [2 5]
              [3 5]
              [4 5]})))))


(deftest test-aggregates
  (let [monsters [ ["Cerberus" 3]
                   ["Medusa" 1]
                   ["Cyclops" 1]
                   ["Chimera" 1] ]]
    (testing "with"
      (is (= (d/q '[ :find ?heads 
                     :with ?monster 
                     :in   [[?monster ?heads]] ]
                  [ ["Medusa" 1]
                    ["Cyclops" 1]
                    ["Chimera" 1] ])
             [[1] [1] [1]])))
    
    (testing "Wrong grouping without :with"
      (is (= (d/q '[ :find (sum ?heads)
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[4]])))
    
    (testing "Multiple aggregates, correct grouping with :with"
      (is (= (d/q '[ :find (sum ?heads) (min ?heads) (max ?heads) (count ?heads)
                     :with ?monster
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[6 1 3 4]])))

    (testing "Grouping and parameter passing"
      (is (= (set (d/q '[ :find ?color (max ?amount ?x) (min ?amount ?x)
                          :in   [[?color ?x]] ?amount ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       3))
             #{[:red  [3 4 5] [1 2 3]]
               [:blue [7 8]   [7 8]]})))
    
    (testing "Custom aggregates"
      (is (= (set (d/q '[ :find ?color (?agg ?x)
                          :in   [[?color ?x]] ?agg ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       #(reverse (sort %))))
             #{[:red [5 4 3 2 1]] [:blue [8 7]]})))))

;; (t/test-ns 'test.datomicscript)

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

;; (measure
;;   #(def big-db (reduce d/with
;;                  (d/empty-db)
;;                  (repeatedly 2000 (fn [] [(random-man)])))))
;; (measure #(d/q '{:find [?e ?a ?s]
;;                  :where [[?e :name "Ivan"]
;;                          [?e :age ?a]
;;                          [?e :sex ?s]]}
;;            big-db))

