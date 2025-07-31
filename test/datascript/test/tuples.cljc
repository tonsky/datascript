(ns datascript.test.tuples
  (:require
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]
    [datascript.test.core :as tdc])
  #?(:clj
     (:import
       [clojure.lang ExceptionInfo])))

(deftest test-schema
  (let [db (d/empty-db
             {:year+session {:db/tupleAttrs [:year :session]}
              :semester+course+student {:db/tupleAttrs [:semester :course :student]}
              :session+student {:db/tupleAttrs [:session :student]
                                :db/valueType :db.type/tuple}})]
    (is (= #{:year+session :semester+course+student :session+student}
          (:db.type/tuple (:rschema db))))

    (is (= {:year     {:year+session 0}
            :session  {:year+session 1, :session+student 0}
            :semester {:semester+course+student 0}
            :course   {:semester+course+student 1}
            :student  {:semester+course+student 2, :session+student 1}}
          (:db/attrTuples (:rschema db))))

    (is (thrown-msg? ":t2 :db/tupleAttrs can’t depend on another tuple attribute: :t1"
          (d/empty-db {:t1 {:db/tupleAttrs [:a :b]}
                       :t2 {:db/tupleAttrs [:c :d :e :t1]}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs must be a sequential collection, got: :a"
          (d/empty-db {:t1 {:db/tupleAttrs :a}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs can’t be empty"
          (d/empty-db {:t1 {:db/tupleAttrs ()}})))

    (is (thrown-msg? ":t1 has :db/tupleAttrs, must be :db.cardinality/one"
          (d/empty-db {:t1 {:db/tupleAttrs [:a :b :c]
                            :db/cardinality :db.cardinality/many}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs can’t depend on :db.cardinality/many attribute: :a"
          (d/empty-db {:a  {:db/cardinality :db.cardinality/many}
                       :t1 {:db/tupleAttrs [:a :b :c]}}))))
  (is (thrown-msg? "Bad attribute specification for :foo+bar: {:db/valueType :db.type/tuple} should also have :db/tupleAttrs"
        (d/empty-db {:foo+bar {:db/valueType :db.type/tuple}}))))

(deftest test-tx
  (let [conn (d/create-conn {:a+b   {:db/tupleAttrs [:a :b]}
                             :a+c+d {:db/tupleAttrs [:a :c :d]}})]
    (are [tx datoms] (= datoms (tdc/all-datoms (:db-after (d/transact! conn tx))))
      [[:db/add 1 :a "a"]]
      #{[1 :a     "a"]
        [1 :a+b   ["a" nil]]
        [1 :a+c+d ["a" nil nil]]}

      [[:db/add 1 :b "b"]]
      #{[1 :a     "a"]
        [1 :b     "b"]
        [1 :a+b   ["a" "b"]]
        [1 :a+c+d ["a" nil nil]]}

      [[:db/add 1 :a "A"]]
      #{[1 :a     "A"]
        [1 :b     "b"]
        [1 :a+b   ["A" "b"]]
        [1 :a+c+d ["A" nil nil]]}

      [[:db/add 1 :c "c"]
       [:db/add 1 :d "d"]]
      #{[1 :a     "A"]
        [1 :b     "b"]
        [1 :a+b   ["A" "b"]]
        [1 :c     "c"]
        [1 :d     "d"]
        [1 :a+c+d ["A" "c" "d"]]}

      [[:db/add 1 :a "a"]]
      #{[1 :a     "a"]
        [1 :b     "b"]
        [1 :a+b   ["a" "b"]]
        [1 :c     "c"]
        [1 :d     "d"]
        [1 :a+c+d ["a" "c" "d"]]}

      [[:db/add 1 :a "A"]
       [:db/add 1 :b "B"]
       [:db/add 1 :c "C"]
       [:db/add 1 :d "D"]]
      #{[1 :a     "A"]
        [1 :b     "B"]
        [1 :a+b   ["A" "B"]]
        [1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d ["A" "C" "D"]]}

      [[:db/retract 1 :a "A"]]
      #{[1 :b     "B"]
        [1 :a+b   [nil "B"]]
        [1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d [nil "C" "D"]]}

      [[:db/retract 1 :b "B"]]
      #{[1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d [nil "C" "D"]]})

    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"A\" \"B\"]]"
          (d/transact! conn [{:db/id 1 :a+b ["A" "B"]}])))))

(deftest test-ignore-correct
  (let [conn (d/create-conn {:a+b {:db/tupleAttrs [:a :b]}})]
    (testing "insert"
      (d/transact! conn [{:db/id 1 :a "a" :b "b" :a+b ["a" "b"]}])
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" \"b\"]]"
            (d/transact! conn [{:db/id 2 :a "x" :b "y" :a+b ["a" "b"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" \"b\"]]"
            (d/transact! conn [{:db/id 2 :a+b ["a" "b"] :a "x" :b "y"}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\"]]"
            (d/transact! conn [{:db/id 2 :a "a" :b "b" :a+b ["a"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" nil]]"
            (d/transact! conn [{:db/id 2 :a "a" :b "b" :a+b ["a" nil]}]))))

    (testing "update"
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" \"b\"]]"
            (d/transact! conn [{:db/id 1 :a "x" :a+b ["a" "b"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" \"B\"]]"
            (d/transact! conn [{:db/id 1 :a+b ["a" "B"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\"]]"
            (d/transact! conn [{:db/id 1 :a "a" :b "b" :a+b ["a"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
            (d/transact! conn [{:db/id 1 :a "a" :b "b" :a+b ["a" nil]}])))
      (d/transact! conn [{:db/id 1 :a+b ["a" "b"]}])
      (d/transact! conn [{:db/id 1 :b "B" :a+b ["a" "B"]}])
      (d/transact! conn [{:db/id 1 :a+b ["A" "B"] :a "A"}]))))

(deftest test-unique
  (let [conn (d/create-conn {:a+b {:db/tupleAttrs [:a :b]
                                   :db/unique :db.unique/identity}})]
    (d/transact! conn [[:db/add 1 :a "a"]])
    (d/transact! conn [[:db/add 2 :a "A"]])
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
          (d/transact! conn [[:db/add 1 :a "A"]])))

    (d/transact! conn [[:db/add 1 :b "b"]
                       [:db/add 2 :b "b"]
                       {:db/id 3 :a "a" :b "B"}])

    (is (= #{[1 :a "a"]
             [1 :b "b"]
             [1 :a+b ["a" "b"]]
             [2 :a "A"]
             [2 :b "b"]
             [2 :a+b ["A" "b"]]
             [3 :a "a"]
             [3 :b "B"]
             [3 :a+b ["a" "B"]]}
          (tdc/all-datoms (d/db conn))))

    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
          (d/transact! conn [[:db/add 1 :a "A"]])))
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
          (d/transact! conn [[:db/add 1 :b "B"]])))
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
          (d/transact! conn [[:db/add 1 :a "A"]
                             [:db/add 1 :b "B"]])))

    (testing "multiple tuple updates"
      ;; changing both tuple components in a single operation
      (d/transact! conn [{:db/id 1 :a "A" :b "B"}])
      (is (= {:db/id 1 :a "A" :b "B" :a+b ["A" "B"]}
            (d/pull (d/db conn) '[*] 1)))

      ;; adding entity with two tuple components in a single operation
      (d/transact! conn [{:db/id 4 :a "a" :b "b"}])
      (is (= {:db/id 4 :a "a" :b "b" :a+b ["a" "b"]}
            (d/pull (d/db conn) '[*] 4))))))

(deftest test-upsert
  (let [conn (d/create-conn {:a+b {:db/tupleAttrs [:a :b]
                                   :db/unique :db.unique/identity}
                             :c   {:db/unique :db.unique/identity}})]
    (d/transact! conn
      [{:db/id 1 :a "A" :b "B"}
       {:db/id 2 :a "a" :b "b"}])

    (d/transact! conn [{:a+b ["A" "B"] :c "C"}
                       {:a+b ["a" "b"] :c "c"}])
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :c "C"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
          (tdc/all-datoms (d/db conn))))  

    (is (thrown-msg? "Conflicting upserts: [:a+b [\"A\" \"B\"]] resolves to 1, but [:c \"c\"] resolves to 2"
          (d/transact! conn [{:a+b ["A" "B"] :c "c"}])))

    ;; change tuple + upsert
    (d/transact! conn
      [{:a+b ["A" "B"]
        :b "b"
        :d "D"}])

    (is (= #{[1 :a "A"]
             [1 :b "b"]
             [1 :a+b ["A" "b"]]
             [1 :c "C"]
             [1 :d "D"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
          (tdc/all-datoms (d/db conn)))))

  (testing "#378"
    (let [conn (d/create-conn {:player {:db/unique :db.unique/identity}
                               :home {:db/valueType :db.type/ref}
                               :away {:db/valueType :db.type/ref}
                               :players {:db/unique :db.unique/identity
                                         :db/tupleAttrs [:home :away]}})]
      (d/transact! conn [[:db/add -1 :player "Nadal"]
                         [:db/add -2 :player "Federer"]
                         {:home -1
                          :away -2}])
      (d/transact! conn [{:db/id "match"
                          :players ["p1" "p2"]
                          :game 3}
                         {:db/id "p1"
                          :player "Nadal"}
                         {:db/id "p2"
                          :player "Federer"}])

      (is (= 3 (:game (d/entity @conn 3)))
          "Upsert successful"))))

;; issue-473
(deftest test-upsert-by-tuple-components
  (let [db   (d/empty-db {:a+b {:db/tupleAttrs [:a :b]
                                :db/unique :db.unique/identity}})
        db'  (d/db-with db [{:a "A" :b "B" :name "Ivan"}])]
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :name "Oleg"]}
          (tdc/all-datoms
            (d/db-with db'
              [{:db/id -1 :a "A" :b "B" :name "Oleg"}]))))
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :name "Oleg"]}
          (tdc/all-datoms
            (d/db-with db'
              [{:a "A" :b "B" :name "Oleg"}]))))
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :name "Oleg"]}
          (tdc/all-datoms
            (d/db-with db'
              [[:db/add -1 :a "A"]
               [:db/add -1 :b "B"] 
               [:db/add -1 :name "Oleg"]]))))))

(deftest test-lookup-refs
  (let [conn (d/create-conn {:a+b {:db/tupleAttrs [:a :b]
                                   :db/unique :db.unique/identity}
                             :c   {:db/unique :db.unique/identity}})]
    (d/transact! conn
      [{:db/id 1 :a "A" :b "B"}
       {:db/id 2 :a "a" :b "b"}])

    (d/transact! conn [[:db/add [:a+b ["A" "B"]] :c "C"]
                       {:db/id [:a+b ["a" "b"]] :c "c"}])
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :c "C"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
          (tdc/all-datoms (d/db conn))))  

    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
          (d/transact! conn [[:db/add [:a+b ["A" "B"]] :c "c"]])))

    (is (thrown-msg? "Conflicting upsert: [:c \"c\"] resolves to 2, but entity already has :db/id 1"
          (d/transact! conn [{:db/id [:a+b ["A" "B"]] :c "c"}])))

    ;; change tuple + upsert
    (d/transact! conn
      [{:db/id [:a+b ["A" "B"]]
        :b "b"
        :d "D"}])

    (is (= #{[1 :a "A"]
             [1 :b "b"]
             [1 :a+b ["A" "b"]]
             [1 :c "C"]
             [1 :d "D"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
          (tdc/all-datoms (d/db conn))))

    (is (= {:db/id 2
            :a     "a"
            :b     "b"
            :a+b   ["a" "b"]
            :c     "c"}
          (d/pull (d/db conn) '[*] [:a+b ["a" "b"]])))))

;; issue-452
(deftest lookup-refs-in-tuple
  (let [schema {:ref      {:db/valueType :db.type/ref}
                :name     {:db/unique :db.unique/identity}
                :ref+name {:db/valueType :db.type/tuple
                           :db/tupleAttrs [:ref :name]
                           :db/unique :db.unique/identity}}
        db     (-> (d/empty-db schema)
                 (d/db-with
                   [{:db/id -1 :name "Ivan"}
                    {:db/id -2 :name "Oleg"}
                    {:db/id -3 :name "Petr" :ref -1}
                    {:db/id -4 :name "Yuri" :ref -2}]))]
    (let [db' (d/db-with db [{:ref+name [1 "Petr"], :age 32}])]
      (is (= {:age 32} (d/pull db' [:age] 3))))
    
    (let [db' (d/db-with db [{:ref+name [[:name "Ivan"] "Petr"], :age 32}])]
      (is (= {:age 32} (d/pull db' [:age] 3))))
    
    (let [db' (d/db-with db [[:db/add -1 :ref+name [1 "Petr"]]
                             [:db/add -1 :age 32]])]
      (is (= {:age 32} (d/pull db' [:age] 3))))
    
    (let [db' (d/db-with db [[:db/add -1 :ref+name [[:name "Ivan"] "Petr"]]
                             [:db/add -1 :age 32]])]
      (is (= {:age 32} (d/pull db' [:age] 3))))
    
    (is (= 1 (:db/id (d/entity db [:name "Ivan"]))))
    (is (= 3 (:db/id (d/entity db [:ref+name [1 "Petr"]]))))
    (is (= 3 (:db/id (d/entity db [:ref+name [[:name "Ivan"] "Petr"]]))))))

(deftest test-validation
  (let [db  (d/empty-db {:a+b {:db/tupleAttrs [:a :b]}})
        db1 (d/db-with db [[:db/add 1 :a "a"]])]
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [nil nil]]"
          (d/db-with db [[:db/add 1 :a+b [nil nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
          (d/db-with db1 [[:db/add 1 :a+b ["a" nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
          (d/db-with db [[:db/add 1 :a "a"]
                         [:db/add 1 :a+b ["a" nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/retract 1 :a+b [\"a\" nil]]"
          (d/db-with db1 [[:db/retract 1 :a+b ["a" nil]]])))))

(deftest test-indexes
  (let [db (-> (d/empty-db {:a+b+c {:db/tupleAttrs [:a :b :c]}})
             (d/db-with
               [{:db/id 1 :a "a" :b "b" :c "c"}
                {:db/id 2 :a "A" :b "b" :c "c"}
                {:db/id 3 :a "a" :b "B" :c "c"}
                {:db/id 4 :a "A" :b "B" :c "c"}
                {:db/id 5 :a "a" :b "b" :c "C"}
                {:db/id 6 :a "A" :b "b" :c "C"}
                {:db/id 7 :a "a" :b "B" :c "C"}
                {:db/id 8 :a "A" :b "B" :c "C"}]))]
    (is (= [6]
          (mapv :e (d/datoms db :avet :a+b+c ["A" "b" "C"]))))
    (is (= []
          (mapv :e (d/datoms db :avet :a+b+c ["A" "b" nil]))))
    (is (= [8 4 6 2]
          (mapv :e (d/index-range db :a+b+c ["A" "B" "C"] ["A" "b" "c"]))))
    (is (= [8 4]
          (mapv :e (d/index-range db :a+b+c ["A" "B" nil] ["A" "b" nil]))))))

(deftest test-queries
  (let [db (-> (d/empty-db {:a+b {:db/tupleAttrs [:a :b]
                                  :db/unique :db.unique/identity}})
             (d/db-with [{:db/id 1 :a "A" :b "B"}
                         {:db/id 2 :a "A" :b "b"}
                         {:db/id 3 :a "a" :b "B"}
                         {:db/id 4 :a "a" :b "b"}]))]
    (is (= #{[3]}
          (d/q '[:find ?e
                 :where [?e :a+b ["a" "B"]]] db)))

    (is (= #{[["a" "B"]]}
          (d/q '[:find ?a+b
                 :where [[:a+b ["a" "B"]] :a+b ?a+b]] db)))

    (is (= #{[["A" "B"]] [["A" "b"]] [["a" "B"]] [["a" "b"]]}
          (d/q '[:find ?a+b
                 :where [?e :a ?a]
                 [?e :b ?b]
                 [(tuple ?a ?b) ?a+b]] db)))

    (is (= #{["A" "B"] ["A" "b"] ["a" "B"] ["a" "b"]}
          (d/q '[:find ?a ?b
                 :where [?e :a+b ?a+b]
                 [(untuple ?a+b) [?a ?b]]] db)))))
