(ns datascript.test.query-fns
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc])
#?(:clj
   (:import [clojure.lang ExceptionInfo])))

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
                    :where [?e :age ?age]
                           [(get-else $ ?e :height 300) ?height]] db)
             #{[1 15 300] [2 22 240] [3 37 300]}))
      
      (is (thrown-with-msg? ExceptionInfo #"get-else: nil default value is not supported"
            (d/q '[:find ?e ?height
                    :where [?e :age]
                           [(get-else $ ?e :height nil) ?height]] db))))

    (testing "get-some"
      (is (= (d/q '[:find ?e ?a ?v
                    :where [?e :name _]
                           [(get-some $ ?e :height :age) [?a ?v]]] db)
             #{[1 :age 15]
               [2 :height 240]
               [3 :age 37]})))

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

    (testing "Built-ins"
      (is (= (d/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [(< ?a1 18 ?a2)]] db)
             #{[1 2] [1 3]}))
      
      (is (= (d/q '[:find  ?x ?c
                    :in    [?x ...]
                    :where [(count ?x) ?c]]
                   ["a" "abc"])
             #{["a" 1] ["abc" 3]})))

    (testing "Built-in vector, hashmap"
      (is (= (d/q '[:find [?tx-data ...]
                    :where
                    [(ground :db/add) ?op]
                    [(vector ?op -1 :attr 12) ?tx-data]])
             [[:db/add -1 :attr 12]]))

      (is (= (d/q '[:find [?tx-data ...]
                    :where
                    [(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]])
             [{:db/id -1 :age 92 :name "Aaron"}])))


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

    (testing "Two conflicting function values for one binding."
      (is (= (d/q '[:find  ?n
                    :where [(identity 1) ?n]
                           [(identity 2) ?n]]
                  db)
             #{})))

    (testing "Destructured conflicting function values for two bindings."
      (is (= (d/q '[:find  ?n ?x
                    :where [(identity [3 4]) [?n ?x]]
                           [(identity [1 2]) [?n ?x]]]
                  db)
             #{})))

    (testing "Rule bindings interacting with function binding. (fn, rule)"
      (is (= (d/q '[:find  ?n
                    :in $ %
                    :where [(identity 2) ?n]
                    (my-vals ?n)]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]])
             #{[2]})))

    (testing "Rule bindings interacting with function binding. (rule, fn)"
      (is (= (d/q '[:find  ?n
                    :in $ %
                    :where (my-vals ?n)
                    [(identity 2) ?n]]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]])
             #{[2]})))

    (testing "Conflicting relational bindings with function binding. (rel, fn)"
      (is (= (d/q '[:find  ?age
                    :where [_ :age ?age]
                           [(identity 100) ?age]]
                  db)
             #{})))

    (testing "Conflicting relational bindings with function binding. (fn, rel)"
      (is (= (d/q '[:find  ?age
                    :where [(identity 100) ?age]
                           [_ :age ?age]]
                  db)
             #{})))

    (testing "Function on empty rel"
      (is (= (d/q '[:find  ?e ?y
                    :where [?e :salary ?x]
                           [(+ ?x 100) ?y]]
                  [[0 :age 15] [1 :age 35]])
             #{})))
    
    (testing "Returning nil from function filters out tuple from result"
      (is (= (d/q '[:find ?x
                    :in    [?in ...] ?f
                    :where [(?f ?in) ?x]]
                [1 2 3 4]
                #(when (even? %) %))
           #{[2] [4]})))

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


(deftest test-predicates
  (let [entities [{:db/id 1 :name "Ivan" :age 10}
                  {:db/id 2 :name "Ivan" :age 20}
                  {:db/id 3 :name "Oleg" :age 10}
                  {:db/id 4 :name "Oleg" :age 20}]
        db (d/db-with (d/empty-db) entities)]
    (are [q res] (= (d/q (quote q) db) res)
      ;; plain predicate
      [:find  ?e ?a
       :where [?e :age ?a]
              [(> ?a 10)]]
      #{[2 20] [4 20]}

      ;; join in predicate
      [:find  ?e ?e2
       :where [?e  :name]
              [?e2 :name]
              [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}
         
      ;; join with extra symbols
      [:find  ?e ?e2
       :where [?e  :age ?a]
              [?e2 :age ?a2]
              [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; empty result
      [:find  ?e ?e2
       :where [?e  :name "Ivan"]
              [?e2 :name "Oleg"]
              [(= ?e ?e2)]]
      #{}

      ;; pred over const, true
      [:find  ?e
       :where [?e :name "Ivan"]
              [?e :age 20]
              [(= ?e 2)]]
      #{[2]}

      ;; pred over const, false
      [:find  ?e
       :where [?e :name "Ivan"]
              [?e :age 20]
              [(= ?e 1)]]
      #{})
    (let [pred (fn [db e a]
                 (= a (:age (d/entity db e))))]
      (is (= (d/q '[:find ?e
                    :in $ ?pred
                    :where [?e :age ?a]
                           [(?pred $ ?e 10)]]
                  db pred)
             #{[1] [3]})))))


(deftest test-exceptions
  (is (thrown-with-msg? ExceptionInfo #"Unknown predicate 'fun in \[\(fun \?e\)\]"
    (d/q '[:find ?e
           :in   [?e ...]
           :where [(fun ?e)]]
         [1])))
  
  (is (thrown-with-msg? ExceptionInfo #"Unknown function 'fun in \[\(fun \?e\) \?x\]"
    (d/q '[:find ?e ?x
           :in   [?e ...]
           :where [(fun ?e) ?x]]
         [1]))))

(deftest test-issue-180
  (is (= #{}
         (d/q '[:find ?e ?a
                :where [_ :pred ?pred]
                       [?e :age ?a]
                       [(?pred ?a)]]
              (d/db-with (d/empty-db) [[:db/add 1 :age 20]])))))

(defn sample-query-fn [] 42)

#?(:clj
(deftest test-symbol-resolution
  (is (= 42 (d/q '[:find ?x .
                   :where [(datascript.test.query-fns/sample-query-fn) ?x]])))))
