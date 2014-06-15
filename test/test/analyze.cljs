(ns test.analyze
  (:require-macros
    [cemerick.cljs.test :refer (is deftest with-test run-tests testing test-var)])
  (:require
    [cemerick.cljs.test :as t]
    [datascript :as d]))

(enable-console-print!)

(deftest test-with
  (let [db  (-> (d/empty-db {:aka { :cardinality :many }})
                (d/with [[:db/add 1 :name "Ivan"]])
                (d/with [[:db/add 1 :name "Petr"]])
                (d/with [[:db/add 1 :aka  "Devil"]])
                (d/with [[:db/add 1 :aka  "Tupen"]]))]

    (is (= (d/analyze-q '[:find ?v
                  :where [1 :name ?v]] db)
           #{[db :eavt 1 :name]}))
    (is (= (d/analyze-q '[:find ?v
                  :where [1 :aka ?v]] db)
           #{[db :eavt 1 :aka]}))))

(deftest test-joins
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan", :age   15 }
                         { :db/id 2, :name  "Petr", :age   37 }
                         { :db/id 3, :name  "Ivan", :age   37 }
                         { :db/id 4, :age 15 }]))]
    (is (= (d/analyze-q '[:find ?e
                  :where [?e :name]] db)
           #{[db :avet :name]}))
    (is (= (d/analyze-q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                         [?e :age ?v]] db)
           #{[db :avet :name "Ivan"][db :avet :age]}))
    (is (= (d/analyze-q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                         [?e2 :name ?n]] db)
           #{[db :avet :name]}))
    (is (= (d/analyze-q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                         [?e :age ?a]
                         [?e2 :age ?a]
                         [?e2 :name ?n]] db)
           #{[db :avet :name "Ivan"][db :avet :age][db :avet :name]}))))

(deftest test-q-coll
  (let [db [ [1 :name "Ivan"]
             [1 :age  19]
             [1 :aka  "dragon_killer_94"]
             [1 :aka  "-=autobot=-"] ] ]
    (is (= (d/analyze-q '[ :find  ?n ?a
                   :where [?e :aka "dragon_killer_94"]
                          [?e :name ?n]
                          [?e :age  ?a]] db)
           #{[db :avet :aka "dragon_killer_94"][db :avet :name][db :avet :age]}))))

(deftest test-q-in
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan", :age   15 }
                         { :db/id 2, :name  "Petr", :age   37 }
                         { :db/id 3, :name  "Ivan", :age   37 }]))
        db2 (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan", :age   15 }
                         { :db/id 2, :name  "Petr", :age   37 }
                         { :db/id 3, :name  "Ivan", :age   37 }]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/analyze-q query db :name "Ivan")
           #{[db :avet :name "Ivan"]}))
    (is (= (d/analyze-q query db :age 37)
           #{[db :avet :age 37]}))

    (testing "Named DB"
      (is (= (d/analyze-q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[db :eavt 1]})))

    (testing "DB join with collection"
      (is (= (d/analyze-q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                           [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[db :avet :name]})))

    (testing "Relation binding"
      (is (= (d/analyze-q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[db :avet :name "Ivan"][db :avet :name "Petr"]})))

    (testing "Tuple binding"
      (is (= (d/analyze-q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                           [?e :age ?age]]
                  db ["Ivan" 37])
             #{[db :avet :name "Ivan"][db :avet :age 37]})))

    (testing "Collection binding"
      (is (= (d/analyze-q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[db :eavt 1 :name][db :eavt 1 :age]})))

    (testing "Query multiple DB's"
    (is (= (d/analyze-q '[:find ?e1 ?e2
                          :in $1 $2
                          :where
                          [$1 ?e1 :name ?n]
                          [$2 ?e2 :name ?n]] db db2)
           #{[db :avet :name][db2 :avet :name]}))))

  (testing "Query without DB"
    (is (= (d/analyze-q '[:find ?a ?b
                  :in   ?a ?b]
                10 20)
           #{}))))

(deftest test-user-funs
  (let [db (-> (d/empty-db)
               (d/with [ { :db/id 1, :name  "Ivan",  :age   15 }
                         { :db/id 2, :name  "Petr",  :age   22 }
                         { :db/id 3, :name  "Slava", :age   37 }]))]
    (testing "Built-in predicate"
      (is (= (d/analyze-q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [(< ?a1 18 ?a2)]] db)
             #{[db :avet :age]}))
      (is (= (d/analyze-q '[:find  ?e1 ?e2
                    :where [(< ?a1 18 ?a2)]
                           [?e1 :age ?a1]
                           [?e2 :age ?a2]] db)
             #{[db :avet :age]})))

    (testing "Passing predicate as source"
      (is (= (d/analyze-q '[:find  ?e
                    :in    $ ?adult
                    :where [?e :age ?a]
                           [(?adult ?a)]]
                  db
                  #(> % 18))
             #{[db :avet :age]})))

    (testing "Calling a function"
      (is (= (d/analyze-q '[:find  ?e1 ?e2 ?e3
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [?e3 :age ?a3]
                           [(+ ?a1 ?a2) ?a12]
                           [(= ?a12 ?a3)]]
                  db)
             #{[db :avet :age]}))
      (is (= (d/analyze-q '[:find  ?e1 ?e2 ?e3
                    :where [(+ ?a1 ?a2) ?a12]
                           [(= ?a12 ?a3)]
                           [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [?e3 :age ?a3]]
                  db)
             #{[db :avet :age]})))))

(deftest test-rules
  (let [db [                  [5 :follow 3]
            [1 :follow 2] [2 :follow 3] [3 :follow 4] [4 :follow 6]
                          [2         :follow           4]]]
    (is (= (d/analyze-q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
               '[[(follow ?x ?y)
                  [?x :follow ?y]]])
           #{[db :avet :follow]}))

    (testing "Rule with branches"
      (is (= (d/analyze-q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e2 ?e1)
                    [?e2 :follow ?e1]]
                   [(follow ?e2 ?e1)
                    [?e2 :follow ?t]
                    [?t  :follow ?e1]]])
             #{[db :eavt 1 :follow][db :avet :follow]})))

    (testing "Recursive rule"
      (is (= (d/analyze-q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e1 ?e2)
                    [?e1 :follow ?e2]]
                   [(follow ?e1 ?e2)
                    [?e1 :follow ?t]
                    (follow ?t ?e2)]])
             #{[db :eavt 1 :follow][db :avet :follow]})))))

(deftest test-aggregates
  (let [db (d/create-conn {})]
    (is (= (d/analyze-q '[ :find (sum ?heads)
                           :where
                           [?e :heads ?heads]]
                        db)
           #{[db :avet :heads]}))))

(deftest test-listen-q!
  (let [conn    (d/create-conn)
        reports (atom [])
        q '[:find ?e ?n :where [?e :name ?n]]]
    (d/transact! conn [[:db/add -1 :name "Alex"]
                       [:db/add -2 :name "Boris"]])
    (d/listen! conn :test #(swap! reports conj %) q [@conn])
    (d/transact! conn [[:db/add -1 :name "Dima"]
                       [:db/add -1 :age 19]
                       [:db/add -2 :name "Evgeny"]])
    (d/transact! conn [[:db/add -1 :name "Fedor"]
                       [:db/add 1 :name "Alex2"]         ;; should update
                       [:db/retract 2 :name "Not Boris"] ;; should be skipped
                       [:db/retract 4 :name "Evgeny"]])
    (d/transact! conn [[:db/add -1 :age 22]]) ;; Should be skipped
    (d/unlisten! conn :test)
    (d/transact! conn [[:db/add -1 :name "Geogry"]])
    (is (= (map (fn [report] (set (map #(into [] %) (:tx-data report)))) @reports)
           [#{[3 :name "Dima"   (+ d/tx0 2) true]
              [3 :age 19        (+ d/tx0 2) true]
              [4 :name "Evgeny" (+ d/tx0 2) true]}
            #{[5 :name "Fedor"  (+ d/tx0 3) true]
              [1 :name "Alex"   (+ d/tx0 3) false] ;; update -> retract
              [1 :name "Alex2"  (+ d/tx0 3) true]  ;;         + add
              [4 :name "Evgeny" (+ d/tx0 3) false]}]))))
