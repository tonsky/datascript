(ns test.datascript
  (:require-macros
    [cemerick.cljs.test :refer (is deftest with-test run-tests testing test-var)])
  (:require
    [cemerick.cljs.test :as t]
    [datascript :as d]
    [datascript.query :as dq]))

(enable-console-print!)

(deftest test-with
  (let [db  (-> (d/empty-db {:aka { :db/cardinality :db.cardinality/many }})
                (d/with [[:db/add 1 :name "Ivan"]])
                (d/with [[:db/add 1 :name "Petr"]])
                (d/with [[:db/add 1 :aka  "Devil"]])
                (d/with [[:db/add 1 :aka  "Tupen"]]))]
    
    (is (= (dq/q '[:find ?v
                  :where [1 :name ?v]] db)
           #{["Petr"]}))
    (is (= (dq/q '[:find ?v
                  :where [1 :aka ?v]] db)
           #{["Devil"] ["Tupen"]}))
    
    (testing "Retract"
      (let [db  (-> db
                  (d/with [[:db/retract 1 :name "Petr"]])
                  (d/with [[:db/retract 1 :aka  "Devil"]]))]

        (is (= (dq/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{}))
        (is (= (dq/q '[:find ?v
                      :where [1 :aka ?v]] db)
               #{["Tupen"]}))
        
        (is (= (d/entity db 1) { :db/id 1, :aka ["Tupen"] }))))
    
    (testing "Cannot retract what's not there"
      (let [db  (-> db
                    (d/with [[:db/retract 1 :name "Ivan"]]))]
        (is (= (dq/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{["Petr"]}))))))

(deftest test-retract-fns
  (let [db (-> (d/empty-db {:aka { :db/cardinality :db.cardinality/many }})
               (d/with [ { :db/id 1, :name  "Ivan", :age 15, :aka ["X" "Y" "Z"] }
                         { :db/id 2, :name  "Petr", :age 37 } ]))]
    (let [db (d/with db [ [:db.fn/retractEntity 1] ])]
      (is (= (dq/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{}))
      (is (= (dq/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))
    
    (let [db (d/with db [ [:db.fn/retractAttribute 1 :name] ])]
      (is (= (dq/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"]}))
      (is (= (dq/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))
    
    (let [db (d/with db [ [:db.fn/retractAttribute 1 :aka] ])]
      (is (= (dq/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:name "Ivan"] [:age 15]}))
      (is (= (dq/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))))


(deftest test-transact!
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many }})]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]])
    
    (is (= (dq/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (dq/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))))

(deftest test-db-fn
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many }})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (dq/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                  db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid :had-birthday true]]
                    (throw (js/Error. (str "No entity with name: " name)))))]
    (d/transact! conn [{:db/id 1 :name "Ivan" :age 31}])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]])
    (is (= (dq/q '[:find ?v ?a
                  :where [?e :name ?v]
                         [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (dq/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-with-msg? js/Error #"No entity with name: Bob"
                          (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
          e (d/entity db-after 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))


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
    (is (= (dq/q '[:find  ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                         [?e :age ?a]] @conn)
           #{[1 "Ivan" 19   (+ d/tx0 1)]
             [2 "Petr" 22   (+ d/tx0 1)]
             [3 "Sergey" 30 (+ d/tx0 2)]}))))

(deftest test-resolve-eid-refs
  (let [conn (d/create-conn {:friend {:db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/many}})
        tx   (d/transact! conn [{:name "Sergey"
                                 :friend [-1 -2]}
                                [:db/add -1 :name "Ivan"]
                                [:db/add -2 :name "Petr"]
                                [:db/add -4 :name "Boris"]
                                [:db/add -4 :friend -3]
                                [:db/add -3 :name "Oleg"]
                                [:db/add -3 :friend -4]])
        q '[:find ?fn
            :in $ ?n
            :where [?e :name ?n]
                   [?e :friend ?fe]
                   [?fe :name ?fn]]]
    (is (= (:tempids tx) { -1 2, -2 3, -4 4, -3 5 }))
    (is (= (dq/q q @conn "Sergey") #{["Ivan"] ["Petr"]}))
    (is (= (dq/q q @conn "Boris") #{["Oleg"]}))
    (is (= (dq/q q @conn "Oleg") #{["Boris"]}))))

(deftest test-entity
  (let [conn (d/create-conn {:aka {:db/cardinality :db.cardinality/many}})
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
  (let [conn (d/create-conn { :aka { :db/cardinality :db.cardinality/many }
                              :also { :db/cardinality :db.cardinality/many} })]
    (d/transact! conn [{:db/id -1
                        :name  "Ivan"
                        :age   16
                        :aka   ["Devil" "Tupen"]
                        :also  "ok"}])
    (is (= (dq/q '[:find  ?n ?a
                  :where [1 :name ?n]
                         [1 :age ?a]] @conn)
           #{["Ivan" 16]}))
    (is (= (dq/q '[:find  ?v
                  :where [1 :also ?v]] @conn)
           #{["ok"]}))
    (is (= (dq/q '[:find  ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))))


(deftest test-joins
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan", :age   15 }
                         { :db/id 2, :name  "Petr", :age   37 }
                         { :db/id 3, :name  "Ivan", :age   37 }
                         { :db/id 4, :age 15 }]))]
    (is (= (dq/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (dq/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                         [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (dq/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                         [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (dq/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                         [?e :age ?a]
                         [?e2 :age ?a]
                         [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))))


(deftest test-q-many
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/with [ [:db/add 1 :name "Ivan"]
                         [:db/add 1 :aka  "ivolga"]
                         [:db/add 1 :aka  "pi"]
                         [:db/add 2 :name "Petr"]
                         [:db/add 2 :aka  "porosenok"]
                         [:db/add 2 :aka  "pi"] ]))]
    (is (= (dq/q '[:find  ?n1 ?n2
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
    (is (= (dq/q '[ :find  ?n ?a
                   :where [?e :aka "dragon_killer_94"]
                          [?e :name ?n]
                          [?e :age  ?a]] db)
           #{["Ivan" 19]})))
  
  (testing "Query over long tuples"
    (let [db [ [1 :name "Ivan" 945 :db/add]
               [1 :age  39     999 :db/retract]] ]
      (is (= (dq/q '[ :find  ?e ?v
                     :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (dq/q '[ :find  ?e ?a ?v ?t
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
    (is (= (dq/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (dq/q query db :age 37)
           #{[2] [3]}))
    
    (testing "Named DB"
      (is (= (dq/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))
    
    (testing "DB join with collection"
      (is (= (dq/q '[:find  ?e ?email
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
      (is (= (dq/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))
    
    (testing "Tuple binding"
      (is (= (dq/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                           [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))
    
    (testing "Collection binding"
      (is (= (dq/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]}))))
  
  (testing "Query without DB"
    (is (= (dq/q '[:find ?a ?b
                  :in   ?a ?b]
                10 20)
           #{[10 20]}))))


(deftest test-nested-bindings
  (is (= (dq/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))
  
  (is (= (dq/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                       [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))
  
  (is (= (dq/q '[:find  ?k ?x
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
      (is (= (dq/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [(< ?a1 18 ?a2)]] db)
             #{[1 2] [1 3]})))
    
    (testing "Passing predicate as source"
      (is (= (dq/q '[:find  ?e
                    :in    $ ?adult
                    :where [?e :age ?a]
                           [(?adult ?a)]]
                  db
                  #(> % 18))
             #{[2] [3]})))
    
    (testing "Calling a function"
      (is (= (dq/q '[:find  ?e1 ?e2 ?e3
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
    (is (= (dq/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
               '[[(follow ?x ?y)
                  [?x :follow ?y]]])
           #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))
    
    (testing "Rule with branches"
      (is (= (dq/q '[:find  ?e2
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
    
    (testing "Recursive rules"
      (is (= (dq/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e1 ?e2)
                    [?e1 :follow ?e2]]
                   [(follow ?e1 ?e2)
                    [?e1 :follow ?t]
                    (follow ?t ?e2)]])
             #{[2] [3] [4] [6]}))
      
      (is (= (dq/q '[:find ?e1 ?e2
                     :in $ %
                     :where (follow ?e1 ?e2)]
                    [[1 :follow 2] [2 :follow 3]]
                   '[[(follow ?e1 ?e2)
                      [?e1 :follow ?e2]]
                     [(follow ?e1 ?e2)
                      (follow ?e2 ?e1)]])
           #{[1 2] [2 3] [2 1] [3 2]}))
      
      (is (= (dq/q '[:find ?e1 ?e2
                     :in $ %
                     :where (follow ?e1 ?e2)]
                    [[1 :follow 2] [2 :follow 3] [3 :follow 1]]
                   '[[(follow ?e1 ?e2)
                      [?e1 :follow ?e2]]
                     [(follow ?e1 ?e2)
                      (follow ?e2 ?e1)]])
           #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]})))
        
    (testing "Mutually recursive rules"
      (is (= (dq/q '[:find  ?e1 ?e2
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
              [4 5]}))))
  
  (testing "Specifying db to rule"
    (is (= (dq/q '[ :find ?n
                    :in   $sexes $ages %
                    :where ($sexes male ?n)
                           ($ages adult ?n) ]
                  [["Ivan" :male] ["Darya" :female] ["Oleg" :male] ["Igor" :male]]
                  [["Ivan" 15] ["Oleg" 66] ["Darya" 32]]
                  '[[(male ?x)
                     [?x :male]]
                    [(adult ?y)
                     [?y ?a]
                     [(>= ?a 18)]]])
           #{["Oleg"]}))))

(deftest test-aggregates
  (let [monsters [ ["Cerberus" 3]
                   ["Medusa" 1]
                   ["Cyclops" 1]
                   ["Chimera" 1] ]]
    (testing "with"
      (is (= (dq/q '[ :find ?heads 
                     :with ?monster 
                     :in   [[?monster ?heads]] ]
                  [ ["Medusa" 1]
                    ["Cyclops" 1]
                    ["Chimera" 1] ])
             [[1] [1] [1]])))
    
    (testing "Wrong grouping without :with"
      (is (= (dq/q '[ :find (sum ?heads)
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[4]])))
    
    (testing "Multiple aggregates, correct grouping with :with"
      (is (= (dq/q '[ :find (sum ?heads) (min ?heads) (max ?heads) (count ?heads)
                     :with ?monster
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[6 1 3 4]])))

    (testing "Grouping and parameter passing"
      (is (= (set (dq/q '[ :find ?color (max ?amount ?x) (min ?amount ?x)
                          :in   [[?color ?x]] ?amount ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       3))
             #{[:red  [3 4 5] [1 2 3]]
               [:blue [7 8]   [7 8]]})))
    
    (testing "Custom aggregates"
      (is (= (set (dq/q '[ :find ?color (?agg ?x)
                          :in   [[?color ?x]] ?agg ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       #(reverse (sort %))))
             #{[:red [5 4 3 2 1]] [:blue [8 7]]})))))

(deftest test-datoms
  (let [dvec #(vector (.-e %) (.-a %) (.-v %))
        db (-> (d/empty-db)
               (d/with [[:db/add 1 :name "Petr"]
                        [:db/add 1 :age 44]
                        [:db/add 2 :name "Ivan"]
                        [:db/add 2 :age 25]
                        [:db/add 3 :name "Sergey"]
                        [:db/add 3 :age 11]]))]
    (testing "Main indexes, sort order"
      (is (= (map dvec (d/datoms db :aevt))
             [ [1 :age 44]
               [2 :age 25]
               [3 :age 11]
               [1 :name "Petr"]
               [2 :name "Ivan"]
               [3 :name "Sergey"] ]))

      (is (= (map dvec (d/datoms db :eavt))
             [ [1 :age 44]
               [1 :name "Petr"]
               [2 :age 25]      
               [2 :name "Ivan"]
               [3 :age 11]
               [3 :name "Sergey"] ]))

      (is (= (map dvec (d/datoms db :avet))
             [ [3 :age 11]
               [2 :age 25]
               [1 :age 44]
               [2 :name "Ivan"]
               [1 :name "Petr"]
               [3 :name "Sergey"] ])))
    
    (testing "Components filtration"
      (is (= (map dvec (d/datoms db :eavt 1))
             [ [1 :age 44]
               [1 :name "Petr"] ]))

      (is (= (map dvec (d/datoms db :eavt 1 :age))
             [ [1 :age 44] ]))

      (is (= (map dvec (d/datoms db :avet :age))
             [ [3 :age 11]
               [2 :age 25]
               [1 :age 44] ])))))

(deftest test-seek-datoms
  (let [dvec #(vector (.-e %) (.-a %) (.-v %))
        db (-> (d/empty-db)
               (d/with [[:db/add 1 :name "Petr"]
                        [:db/add 1 :age 44]
                        [:db/add 2 :name "Ivan"]
                        [:db/add 2 :age 25]
                        [:db/add 3 :name "Sergey"]
                        [:db/add 3 :age 11]]))]
    
    (testing "Non-termination"
      (is (= (map dvec (d/seek-datoms db :avet :age 10))
             [ [3 :age 11]
               [2 :age 25]
               [1 :age 44]
               [2 :name "Ivan"]
               [1 :name "Petr"]
               [3 :name "Sergey"] ])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "P"))
             [ [1 :name "Petr"]
               [3 :name "Sergey"] ])))
    
    (testing "Exact value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "Petr"))
             [ [1 :name "Petr"]
               [3 :name "Sergey"] ])))))

(deftest test-pr-read
  (binding [cljs.reader/*tag-table* (atom {"datascript/Datom" d/datom-from-reader})]
    (let [d (d/Datom. 1 :name 3 17 true)]
      (is (= d (cljs.reader/read-string (pr-str d)))))
    (let [d (d/Datom. 1 :name 3 nil nil)]
      (is (= d (cljs.reader/read-string (pr-str d))))))
  
  (let [db (-> (d/empty-db)
               (d/with [[:db/add 1 :name "Petr"]
                        [:db/add 1 :age 44]
                        [:db/add 2 :name "Ivan"]
                        [:db/add 2 :age 25]
                        [:db/add 3 :name "Sergey"]
                        [:db/add 3 :age 11]]))]
    (binding [cljs.reader/*tag-table* (atom {"datascript/DB" d/db-from-reader})]
      (is (= db (cljs.reader/read-string (pr-str db)))))))

;; (t/test-ns 'test.datascript)
