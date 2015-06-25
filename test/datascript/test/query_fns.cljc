(ns datascript.test.query-fns
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.core :as tdc]))

(deftest test-query-fns
  (testing "predicate without free variables"
    (is (= (d/q '[:find ?x
                  :in [?x ...]
                  :where [(> 2 1)]] [:a :b :c])
           #{[:a] [:b] [:c]})))

  (let [db (-> (d/empty-db {:parent {:db/valueType :db.type/ref}})
               (d/db-with [ { :db/id 1, :name  "Ivan",  :age   15 }
                            { :db/id 2, :name  "Petr",  :age   22, :height 240, :parent 1}
                            { :db/id 3, :name  "Slava", :age   37, :parent 2}]))]

    (testing "ground"
      (is (= (d/q '[:find ?vowel
                    :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
             #{[:a] [:e] [:i] [:o] [:u]})))

    (testing "get-else"
      (is (= (d/q '[:find ?e ?age ?height
                    :in $
                    :where [?e :age ?age]
                           [(get-else $ ?e :height 300) ?height]] db)
             #{[1 15 300] [2 22 240] [3 37 300]})))

    (testing "get-some"
      (is (= (d/q '[:find ?e ?v
                    :in $
                    :where [?e :name ?name]
                           [(get-some $ ?e :height :age) ?v]] db)
             #{[1 15] [2 240] [3 37]})))

    (testing "missing?"
      (is (= (d/q '[:find ?e ?age
                    :in $
                    :where [?e :age ?age]
                           [(missing? $ ?e :height)]] db)
             #{[1 15] [3 37]})))

    (testing "missing? back-ref"
      (is (= (d/q '[:find ?e
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :_parent)]] db)
             #{[3]})))

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
             #{[1 2 3] [2 1 3]})))
    
    (testing "Function on empty rel"
      (is (= (d/q '[:find  ?e ?y
                    :where [?e :salary ?x]
                           [(+ ?x 100) ?y]]
                  [[0 :age 15] [1 :age 35]])
             #{})))

    (testing "Result bindings"
      (is (= (d/q '[:find ?a ?c
                    :in ?in
                    :where [(ground ?in) [?a _ ?c]]]
                  [:a :b :c])
             #{[:a :c]}))
      
      (is (= (d/q '[:find ?in
                    :in ?in
                    :where [(ground ?in) _]]
                  :a)
             #{[:a]}))
      
      (is (= (d/q '[:find ?x ?z
                    :in ?in
                    :where [(ground ?in) [[?x _ ?z]...]]]
                  [[:a :b :c] [:d :e :f]])
             #{[:a :c] [:d :f]}))
      (is (= (d/q '[:find ?in
                    :in [?in ...]
                    :where [(ground ?in) _]]
                  [])
             #{})))
))
