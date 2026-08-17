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
  (is (thrown-msg? "Bad attribute specification for :foo+bar: {:db/valueType :db.type/tuple} should also have :db/tupleAttrs, :db/tupleTypes or :db/tupleType"
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
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" \"b\" \"c\"]]"
            (d/transact! conn [{:db/id 2 :a "a" :b "b" :a+b ["a" "b" "c"]}])))
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

;; issue-378
(deftest test-upsert-by-tuple-with-tempids
  (let [db (-> (d/empty-db {:player  {:db/unique :db.unique/identity}
                            :home    {:db/valueType :db.type/ref}
                            :away    {:db/valueType :db.type/ref}
                            :players {:db/unique :db.unique/identity
                                      :db/tupleAttrs [:home :away]}})
             (d/db-with [[:db/add -1 :player "Nadal"]
                         [:db/add -2 :player "Federer"]
                         {:home -1
                          :away -2}]))
        db' (d/db-with db [{:db/id "p1" :player "Nadal"}
                           {:db/id "p2" :player "Federer"}
                           {:db/id "match"
                            :players ["p1" "p2"]
                            :game 3}])]
    (is (= 3 (:game (d/entity db' 3))))
    (is (= [1 2] (:players (d/entity db' 3))))))

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

;; issue-364

(deftest test-declared-schema
  (let [db (d/empty-db
             {:t1 {:db/tupleAttrs [:a :b]}
              :t2 {:db/valueType :db.type/tuple
                   :db/tupleTypes [:db.type/long :db.type/string]}
              :t3 {:db/valueType :db.type/tuple
                   :db/tupleType :db.type/keyword}})]
    (is (= #{:t1 :t2 :t3} (:db.type/tuple (:rschema db))))
    (is (= #{:t1} (:db/tupleAttrs (:rschema db))))
    (is (= #{:t2} (:db/tupleTypes (:rschema db))))
    (is (= #{:t3} (:db/tupleType (:rschema db))))
    (is (= {:a {:t1 0} :b {:t1 1}} (:db/attrTuples (:rschema db)))))

  (is (thrown-msg? ":t :db/tupleTypes must be a sequential collection of at least 2 keywords, got: :db.type/long"
        (d/empty-db {:t {:db/tupleTypes :db.type/long}})))

  (is (thrown-msg? ":t :db/tupleTypes must be a sequential collection of at least 2 keywords, got: [:db.type/long]"
        (d/empty-db {:t {:db/tupleTypes [:db.type/long]}})))

  (is (thrown-msg? ":t :db/tupleTypes must be a sequential collection of at least 2 keywords, got: [:db.type/long \"long\"]"
        (d/empty-db {:t {:db/tupleTypes [:db.type/long "long"]}})))

  (is (thrown-msg? ":t :db/tupleType must be a keyword, got: [:db.type/long]"
        (d/empty-db {:t {:db/tupleType [:db.type/long]}})))

  (is (thrown-msg? "Bad attribute specification for :t: only one of :db/tupleAttrs, :db/tupleTypes, :db/tupleType is allowed, got [:db/tupleAttrs :db/tupleTypes]"
        (d/empty-db {:t {:db/tupleAttrs [:a :b]
                         :db/tupleTypes [:db.type/long :db.type/long]}})))

  (is (thrown-msg? "Bad attribute specification for :t: only one of :db/tupleAttrs, :db/tupleTypes, :db/tupleType is allowed, got [:db/tupleTypes :db/tupleType]"
        (d/empty-db {:t {:db/tupleTypes [:db.type/long :db.type/long]
                         :db/tupleType :db.type/long}}))))

(deftest test-declared-tx
  (let [conn (d/create-conn {:loc {:db/valueType :db.type/tuple
                                   :db/tupleTypes [:db.type/long :db.type/long]}})]
    (d/transact! conn [[:db/add 1 :loc [100 0]]])
    (is (= #{[1 :loc [100 0]]} (tdc/all-datoms (d/db conn))))

    (d/transact! conn [{:db/id 1 :loc [100 200]}])
    (is (= #{[1 :loc [100 200]]} (tdc/all-datoms (d/db conn))))
    (is (= {:db/id 1 :loc [100 200]} (d/pull (d/db conn) '[*] 1)))
    (is (= [100 200] (:loc (d/entity (d/db conn) 1))))

    (d/transact! conn [{:db/id 1 :loc [100 nil]}])
    (is (= #{[1 :loc [100 nil]]} (tdc/all-datoms (d/db conn))))

    (d/transact! conn [[:db/retract 1 :loc [100 nil]]])
    (is (= #{} (tdc/all-datoms (d/db conn))))

    (is (thrown-msg? "Attribute :loc expected a 2-element vector, got: [1 2 3] in [:db/add 1 :loc [1 2 3]]"
          (d/transact! conn [[:db/add 1 :loc [1 2 3]]])))

    (is (thrown-msg? "Attribute :loc expected a 2-element vector, got: \"blah\" in [:db/add 1 :loc \"blah\"]"
          (d/transact! conn [[:db/add 1 :loc "blah"]])))

    (is (thrown-msg? "Attribute :loc expected a 2-element vector, got: [1 2 3] in [:db/retract 1 :loc [1 2 3]]"
          (d/transact! conn [[:db/retract 1 :loc [1 2 3]]])))))

(deftest test-declared-refs
  (let [schema {:name     {:db/unique :db.unique/identity}
                :ref+long {:db/valueType :db.type/tuple
                           :db/tupleTypes [:db.type/ref :db.type/long]}}
        db     (-> (d/empty-db schema)
                 (d/db-with [{:db/id 1 :name "Ivan"}
                             {:db/id 2 :name "Oleg"}]))]
    (testing "eid in ref slot"
      (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [2 :ref+long [1 7]]}
            (tdc/all-datoms (d/db-with db [[:db/add 2 :ref+long [1 7]]])))))

    (testing "lookup ref in ref slot"
      (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [2 :ref+long [1 7]]}
            (tdc/all-datoms (d/db-with db [[:db/add 2 :ref+long [[:name "Ivan"] 7]]]))))
      (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [2 :ref+long [1 7]]}
            (tdc/all-datoms (d/db-with db [{:db/id 2 :ref+long [[:name "Ivan"] 7]}])))))

    (testing "tempid in ref slot resolves to same entity"
      (let [report (d/with db [{:db/id -1 :name "Petr"}
                               [:db/add 2 :ref+long [-1 7]]])
            petr   (get (:tempids report) -1)]
        (is (= #{[1 :name "Ivan"] [2 :name "Oleg"]
                 [petr :name "Petr"] [2 :ref+long [petr 7]]}
              (tdc/all-datoms (:db-after report))))))

    (testing "tempid used only in ref slot"
      (is (thrown-msg? "Tempids used only as value in transaction: (-5)"
            (d/db-with db [[:db/add 2 :ref+long [-5 7]]]))))

    (testing "extra elements are not silently truncated during ref resolution"
      (is (thrown-msg? "Attribute :ref+long expected a 2-element vector, got: [[:name \"Ivan\"] 7 8] in [:db/add 2 :ref+long [[:name \"Ivan\"] 7 8]]"
            (d/db-with db [[:db/add 2 :ref+long [[:name "Ivan"] 7 8]]]))))

    (testing ":db/current-tx in ref slot"
      (let [db' (:db-after (d/with db [[:db/add 2 :ref+long [:db/current-tx 7]]]))
            tx  (:max-tx db')]
        (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [2 :ref+long [tx 7]]}
              (tdc/all-datoms db')))))

    (testing "retract by value with lookup ref in ref slot"
      (let [db' (d/db-with db [[:db/add 2 :ref+long [1 7]]])]
        (is (= #{[1 :name "Ivan"] [2 :name "Oleg"]}
              (tdc/all-datoms (d/db-with db' [[:db/retract 2 :ref+long [[:name "Ivan"] 7]]]))))))

    (testing "non-ref slots are not resolved"
      (let [db' (d/db-with (d/empty-db {:long+ref {:db/valueType :db.type/tuple
                                                   :db/tupleTypes [:db.type/long :db.type/ref]}
                                        :name     {:db/unique :db.unique/identity}})
                  [{:db/id 1 :name "Ivan"}
                   [:db/add 1 :long+ref [7 [:name "Ivan"]]]])]
        (is (= #{[1 :name "Ivan"] [1 :long+ref [7 1]]}
              (tdc/all-datoms db')))))))

(deftest test-homogeneous
  (let [conn (d/create-conn {:kws {:db/valueType :db.type/tuple
                                   :db/tupleType :db.type/keyword}})]
    (d/transact! conn [[:db/add 1 :kws [:a :b]]])
    (is (= #{[1 :kws [:a :b]]} (tdc/all-datoms (d/db conn))))

    (d/transact! conn [[:db/add 1 :kws [:a :b :c]]])
    (is (= #{[1 :kws [:a :b :c]]} (tdc/all-datoms (d/db conn))))

    (is (thrown-msg? "Attribute :kws expected a vector, got: :a in [:db/add 1 :kws :a]"
          (d/transact! conn [[:db/add 1 :kws :a]]))))

  (testing "homogeneous ref tuple resolves every slot"
    (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}
                              :refs {:db/valueType :db.type/tuple
                                     :db/tupleType :db.type/ref}})
               (d/db-with [{:db/id 1 :name "Ivan"}
                           {:db/id 2 :name "Oleg"}]))]
      (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [1 :refs [1 2]]}
            (tdc/all-datoms (d/db-with db [[:db/add 1 :refs [[:name "Ivan"] [:name "Oleg"]]]])))))))

(deftest test-declared-unique-upsert
  (let [conn (d/create-conn {:name     {:db/unique :db.unique/identity}
                             :ref+long {:db/valueType :db.type/tuple
                                        :db/tupleTypes [:db.type/ref :db.type/long]
                                        :db/unique :db.unique/identity}})]
    (d/transact! conn [{:db/id 1 :name "Ivan"}
                       {:db/id 2 :name "Oleg" :ref+long [1 7]}])

    (testing "upsert by map form"
      (d/transact! conn [{:ref+long [1 7] :age 30}])
      (is (= {:db/id 2 :name "Oleg" :ref+long [1 7] :age 30}
            (d/pull (d/db conn) '[*] 2))))

    (testing "upsert by map form with lookup ref in ref slot"
      (d/transact! conn [{:ref+long [[:name "Ivan"] 7] :age 31}])
      (is (= {:db/id 2 :name "Oleg" :ref+long [1 7] :age 31}
            (d/pull (d/db conn) '[*] 2))))

    (testing "upsert by vector op with tempid"
      (let [report (d/with (d/db conn) [[:db/add -1 :ref+long [[:name "Ivan"] 7]]
                                        [:db/add -1 :age 32]])]
        (is (= 2 (get (:tempids report) -1)))
        (is (= 32 (:age (d/entity (:db-after report) 2))))))

    (testing "upsert by map form with tempid in ref slot"
      (let [report (d/with (d/db conn) [{:db/id -1 :name "Ivan"}
                                        {:ref+long [-1 7] :age 33}])]
        (is (= 1 (get (:tempids report) -1)))
        (is (= 33 (:age (d/entity (:db-after report) 2))))))

    (testing "map form with tempid in ref slot, no existing match"
      (let [report  (d/with (d/db conn) [{:db/id -1 :name "Anna"}
                                         {:ref+long [-1 9] :age 21}])
            anna-id (get (:tempids report) -1)]
        (is (= 21 (:age (d/entity (:db-after report) [:ref+long [anna-id 9]]))))))

    (testing "unique constraint"
      (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
            (d/transact! conn [[:db/add 3 :ref+long [1 7]]]))))

    (testing "conflicting upsert"
      (d/transact! conn [{:db/id 3 :name "Petr" :ref+long [1 8]}])
      (is (thrown-msg? "Conflicting upserts: [:name \"Oleg\"] resolves to 2, but [:ref+long [1 8]] resolves to 3"
            (d/transact! conn [{:name "Oleg" :ref+long [1 8]}]))))))

(deftest test-declared-lookup-refs
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}
                            :ref+long {:db/valueType :db.type/tuple
                                       :db/tupleTypes [:db.type/ref :db.type/long]
                                       :db/unique :db.unique/identity}})
             (d/db-with [{:db/id 1 :name "Ivan"}
                         {:db/id 2 :name "Oleg" :ref+long [1 7]}]))]
    (is (= 2 (:db/id (d/entity db [:ref+long [1 7]]))))
    (is (= 2 (:db/id (d/entity db [:ref+long [[:name "Ivan"] 7]]))))

    (is (= {:name "Oleg"} (d/pull db [:name] [:ref+long [[:name "Ivan"] 7]])))

    (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [2 :ref+long [1 7]] [2 :age 30]}
          (tdc/all-datoms (d/db-with db [[:db/add [:ref+long [[:name "Ivan"] 7]] :age 30]]))))

    (is (= #{[1 :name "Ivan"] [2 :name "Oleg"] [2 :ref+long [1 7]] [2 :age 30]}
          (tdc/all-datoms (d/db-with db [{:db/id [:ref+long [1 7]] :age 30}]))))))

(deftest test-declared-index
  (let [schema {:loc {:db/valueType :db.type/tuple
                      :db/tupleTypes [:db.type/long :db.type/long]}}
        tx     [[:db/add 1 :loc [100 0]]
                [:db/add 2 :loc [100 200]]
                [:db/add 3 :loc [200 0]]]]
    (testing "not indexed by default"
      (let [db (d/db-with (d/empty-db schema) tx)]
        (is (thrown-msg? "Attribute :loc should be marked as :db/index true"
              (vec (d/datoms db :avet :loc))))))

    (testing "indexed with :db/index true"
      (let [db (d/db-with (d/empty-db (update schema :loc assoc :db/index true)) tx)]
        (is (= [1 2 3] (mapv :e (d/datoms db :avet :loc))))
        (is (= [[100 0]] (mapv :v (d/datoms db :avet :loc [100 0]))))
        (is (= [1 2] (mapv :e (d/index-range db :loc [100 0] [100 200]))))))))

(deftest test-declared-cas
  (let [conn (d/create-conn {:name {:db/unique :db.unique/identity}
                             :ref+long {:db/valueType :db.type/tuple
                                        :db/tupleTypes [:db.type/ref :db.type/long]}})]
    (d/transact! conn [{:db/id 1 :name "Ivan"}
                       [:db/add 2 :ref+long [1 7]]])

    (d/transact! conn [[:db.fn/cas 2 :ref+long [[:name "Ivan"] 7] [[:name "Ivan"] 8]]])
    (is (= [1 8] (:ref+long (d/entity (d/db conn) 2))))

    (is (thrown-with-msg? ExceptionInfo #":db.fn/cas failed on datom .*"
          (d/transact! conn [[:db.fn/cas 2 :ref+long [1 7] [1 9]]])))

    (testing "set-if-absent with nil ov"
      (d/transact! conn [[:db.fn/cas 3 :ref+long nil [[:name "Ivan"] 5]]])
      (is (= [1 5] (:ref+long (d/entity (d/db conn) 3)))))

    (testing "arity validated in cas values"
      (is (thrown-msg? "Attribute :ref+long expected a 2-element vector, got: [1 8 9] in [:db.fn/cas 2 :ref+long [1 8 9] [1 9]]"
            (d/transact! conn [[:db.fn/cas 2 :ref+long [1 8 9] [1 9]]]))))))

(deftest test-declared-many
  (let [conn (d/create-conn {:locs {:db/valueType :db.type/tuple
                                    :db/tupleTypes [:db.type/long :db.type/long]
                                    :db/cardinality :db.cardinality/many}})]
    (d/transact! conn [[:db/add 1 :locs [100 0]]
                       [:db/add 1 :locs [200 0]]])
    (is (= #{[1 :locs [100 0]] [1 :locs [200 0]]} (tdc/all-datoms (d/db conn))))

    ;; in map form, multiple values must be wrapped: a naked tuple
    ;; is treated as a collection of values
    (d/transact! conn [{:db/id 2 :locs [[300 0] [400 0]]}])
    (is (= #{[2 :locs [300 0]] [2 :locs [400 0]]}
          (tdc/all-datoms (d/db-with (d/empty-db {:locs {:db/valueType :db.type/tuple
                                                         :db/tupleTypes [:db.type/long :db.type/long]
                                                         :db/cardinality :db.cardinality/many}})
                            [{:db/id 2 :locs [[300 0] [400 0]]}]))))))

(deftest test-declared-serialize
  (let [db  (-> (d/empty-db {:name {:db/unique :db.unique/identity}
                             :loc  {:db/valueType :db.type/tuple
                                    :db/tupleTypes [:db.type/long :db.type/long]}
                             :kws  {:db/valueType :db.type/tuple
                                    :db/tupleType :db.type/keyword}
                             :refs {:db/valueType :db.type/tuple
                                    :db/tupleType :db.type/ref}})
              (d/db-with [{:db/id 1 :name "Ivan" :loc [100 nil] :kws [:a :b]}
                          {:db/id 2 :name "Oleg" :refs [1 1]}]))
        db' (d/from-serializable (d/serializable db))]
    (is (= (tdc/all-datoms db) (tdc/all-datoms db')))
    (is (= (:schema db) (:schema db')))))

;; issue-427

(def read-refs-schema
  {:name     {:db/unique :db.unique/identity}
   :ref      {:db/valueType :db.type/ref}
   :ref+name {:db/valueType :db.type/tuple
              :db/tupleAttrs [:ref :name]
              :db/unique :db.unique/identity}
   :ref+long {:db/valueType :db.type/tuple
              :db/tupleTypes [:db.type/ref :db.type/long]
              :db/index true}
   :long+ref {:db/valueType :db.type/tuple
              :db/tupleTypes [:db.type/long :db.type/ref]
              :db/index true}
   :refs     {:db/valueType :db.type/tuple
              :db/tupleType :db.type/ref
              :db/index true}})

(def *read-refs-db
  (delay
    (-> (d/empty-db read-refs-schema)
      (d/db-with [{:db/id 1 :name "Ivan" :db/ident :ivan}
                  {:db/id 2 :name "Oleg" :ref 1}
                  [:db/add 2 :ref+long [1 7]]
                  [:db/add 2 :long+ref [7 1]]
                  [:db/add 2 :refs [1 2]]]))))

(deftest test-read-refs
  (let [db @*read-refs-db
        es #(mapv :e %)]
    (testing "datoms"
      (are [v] (= [2] (es (d/datoms db :avet :ref+long v)))
        [1 7]
        [[:name "Ivan"] 7]
        [:ivan 7])

      (are [v] (= [2] (es (d/datoms db :eavt 2 :ref+long v)))
        [1 7]
        [[:name "Ivan"] 7]
        [:ivan 7])

      (testing "composite tuples"
        (are [v] (= [2] (es (d/datoms db :avet :ref+name v)))
          [1 "Oleg"]
          [[:name "Ivan"] "Oleg"]
          [:ivan "Oleg"])

        (is (= [1] (es (d/datoms db :avet :ref+name [nil "Ivan"])))))

      (testing "homogeneous tuples resolve every slot"
        (are [v] (= [2] (es (d/datoms db :avet :refs v)))
          [1 2]
          [[:name "Ivan"] [:name "Oleg"]]
          [:ivan [:name "Oleg"]]))

      (testing "non-ref slots are not resolved"
        (is (= [2] (es (d/datoms db :avet :long+ref [7 [:name "Ivan"]]))))
        (is (= [] (es (d/datoms db :avet :long+ref [[:name "Ivan"] 1]))))))

    (testing "seek-datoms, rseek-datoms, find-datom, index-range"
      (is (= [2] (es (take 1 (d/seek-datoms db :avet :ref+long [[:name "Ivan"] 7])))))
      (is (= [2] (es (take 1 (d/rseek-datoms db :avet :ref+long [[:name "Ivan"] 7])))))
      (is (= 2 (:e (d/find-datom db :avet :ref+long [[:name "Ivan"] 7]))))
      (is (= [2] (es (d/index-range db :ref+long [[:name "Ivan"] 7] [[:name "Ivan"] 8]))))
      (is (= [2] (es (d/index-range db :refs [:ivan [:name "Ivan"]] [:ivan [:name "Oleg"]])))))

    (testing "entity, pull"
      (are [eid] (= 2 (:db/id (d/entity db eid)))
        [:ref+name [1 "Oleg"]]
        [:ref+name [[:name "Ivan"] "Oleg"]]
        [:ref+name [:ivan "Oleg"]])

      (is (= {:name "Oleg"} (d/pull db [:name] [:ref+name [:ivan "Oleg"]])))

      (testing "nil value in lookup ref"
        (is (nil? (d/entity db [:ref+name nil]))))

      (testing "extra elements are not silently truncated"
        (is (nil? (d/entity db [:ref+name [1 "Oleg" "extra"]])))))

    (testing "unresolved refs raise"
      (is (thrown-msg? "Nothing found for entity id [:name \"Petr\"]"
            (vec (d/datoms db :avet :ref+long [[:name "Petr"] 7]))))
      (is (thrown-msg? "Nothing found for entity id :petr"
            (vec (d/datoms db :avet :ref+long [:petr 7]))))
      (is (thrown-msg? "Nothing found for entity id [:name \"Petr\"]"
            (d/q '[:find ?e :where [?e :ref+long [[:name "Petr"] 7]]] db))))

    (testing "arity is validated"
      (is (thrown-with-msg? ExceptionInfo #"Attribute :ref\+long expected a 2-element vector, got: \[1\]"
            (vec (d/datoms db :avet :ref+long [1])))))))

(deftest test-read-refs-queries
  (let [db @*read-refs-db]
    (testing "constant in pattern"
      (are [q] (= #{[2]} (d/q q db))
        '[:find ?e :where [?e :ref+long [[:name "Ivan"] 7]]]
        '[:find ?e :where [?e :ref+long [:ivan 7]]]
        '[:find ?e :where [?e :ref+name [[:name "Ivan"] "Oleg"]]]
        '[:find ?e :where [?e :refs [[:name "Ivan"] [:name "Oleg"]]]]
        '[:find ?e :where [?e :long+ref [7 [:name "Ivan"]]]]))

    (testing "tuple passed as an input"
      (is (= #{[2]} (d/q '[:find ?e
                           :in $ ?t
                           :where [?e :ref+long ?t]]
                      db [[:name "Ivan"] 7]))))

    (testing "tuples bound as a collection"
      (is (= #{[2]} (d/q '[:find ?e
                           :in $ [?t ...]
                           :where [?e :ref+long ?t]]
                      db [[[:name "Ivan"] 7] [[:name "Oleg"] 8]])))

      (is (= #{[2]} (d/q '[:find ?e
                           :in $ [?t ...]
                           :where [?e :refs ?t]]
                      db [[:ivan [:name "Oleg"]]]))))

    (testing "tuple in a rule"
      (is (= #{[2]} (d/q '[:find ?e
                           :in $ %
                           :where (ref+long ?e)]
                      db '[[(ref+long ?e)
                            [?e :ref+long [[:name "Ivan"] 7]]]]))))))
