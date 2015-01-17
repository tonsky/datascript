(ns datascript.test.pull-api
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
   [cemerick.cljs.test :as t]
   [datascript :as d]))

(defn- create-test-db
  []
  (-> {:aka    { :db/cardinality :db.cardinality/many }
       :child  { :db/cardinality :db.cardinality/many
                :db/valueType :db.type/ref }
       :friend { :db/cardinality :db.cardinality/many
                :db/valueType :db.type/ref }
       :enemy  { :db/cardinality :db.cardinality/many
                :db/valueType :db.type/ref }
       :father { :db/valueType :db.type/ref }
       
       :part   { :db/valueType :db.type/ref
                :db/isComponent true
                :db/cardinality :db.cardinality/many }}
      d/empty-db
      (d/db-with [[:db/add 1 :name  "Ivan"]
                  [:db/add 1 :name  "Petr"]
                  [:db/add 1 :aka   "Devil"]
                  [:db/add 1 :aka   "Tupen"]
                  [:db/add 2 :name  "David"]
                  [:db/add 3 :name  "Thomas"]
                  [:db/add 4 :name  "Lucy"]
                  [:db/add 5 :name  "Elizabeth"]
                  [:db/add 6 :name  "Matthew"]
                  [:db/add 7 :name  "Eunan"]
                  [:db/add 8 :name  "Kerri"]
                  [:db/add 9 :name  "Rebecca"]
                  [:db/add 1 :child 2]
                  [:db/add 1 :child 3]
                  [:db/add 6 :father 3]
                  
                  [:db/add 10 :name  "Part A"]
                  [:db/add 11 :name  "Part A.A"]
                  [:db/add 10 :part 11]
                  [:db/add 12 :name  "Part A.A.A"]
                  [:db/add 11 :part 12]
                  [:db/add 13 :name  "Part A.A.A.A"]
                  [:db/add 12 :part 13]
                  [:db/add 14 :name  "Part A.A.A.B"]
                  [:db/add 12 :part 14]
                  [:db/add 15 :name  "Part A.B"]
                  [:db/add 10 :part 15]
                  [:db/add 16 :name  "Part A.B.A"]
                  [:db/add 15 :part 16]
                  [:db/add 17 :name  "Part A.B.A.A"]
                  [:db/add 16 :part 17]
                  [:db/add 18 :name  "Part A.B.A.B"]
                  [:db/add 16 :part 18]])))

(deftest test-pull-attr-spec
  (let [db (create-test-db)]
    (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
           (d/pull db '[:name :aka] 1)))

    (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
           (d/pull db '[:name :father :db/id] 6)))

    (is (= [{:name "Petr"} {:name "Elizabeth"}
            {:name "Eunan"} {:name "Rebecca"}]
           (d/pull-many db '[:name] [1 5 7 9])))))

(deftest test-pull-reverse-attr-spec
  (let [db (create-test-db)]
    (is (= {:name "David" :_child [{:db/id 1}]}
           (d/pull db '[:name :_child] 2)))

    (is (= {:name "David" :_child [{:name "Petr"}]}
           (d/pull db '[:name {:_child [:name]}] 2)))))

(deftest test-pull-component-attr
  (let [db    (create-test-db)
        parts {:name "Part A",
               :part
               [{:db/id 11
                 :name "Part A.A",
                 :part
                 [{:db/id 12
                   :name "Part A.A.A",
                   :part
                   [{:db/id 13 :name "Part A.A.A.A"}
                    {:db/id 14 :name "Part A.A.A.B"}]}]}
                {:db/id 15
                 :name "Part A.B",
                 :part
                 [{:db/id 16
                   :name "Part A.B.A",
                   :part
                   [{:db/id 17 :name "Part A.B.A.A"}
                    {:db/id 18 :name "Part A.B.A.B"}]}]}]}]
    (testing "Component entities are expanded recursively"
      (is (= parts (d/pull db '[:name :part] 10))))

    (testing "Reverse component references yield a single result"
      (is (= {:name "Part A.A" :_part {:db/id 10}}
             (d/pull db [:name :_part] 11))))))

(deftest test-pull-wildcard
  (let [db (create-test-db)]
    (is (= {:db/id 1 :name "Petr" :aka ["Devil" "Tupen"]
            :child [{:db/id 2} {:db/id 3}]}
           (d/pull db '[*] 1)))))

(deftest test-pull-limit
  (let [db (-> (create-test-db)
               (d/db-with [[:db/add 4 :friend 5]
                           [:db/add 4 :friend 6]
                           [:db/add 4 :friend 7]
                           [:db/add 4 :friend 8]])
               (d/db-with (for [idx (range 2000)]
                            [:db/add 8 :aka (str "aka-" idx)])))]

    (testing "Without an explicit limit, the default is 1000"
      (is (->> (d/pull db '[:aka] 8)
               :aka
               count
               (= 1000))))

    (testing "Explicit limit can reduce the default"
      (is (->> (d/pull db '[(limit :aka 500)] 8)
               :aka
               count
               (= 500))))

    (testing "Explicit limit can increase the default"
      (is (->> (d/pull db '[(limit :aka 1500)] 8)
               :aka
               count
               (= 1500))))

    (testing "A nil limit produces unlimited results"
      (is (->> (d/pull db '[(limit :aka nil)] 8)
               :aka
               count
               (= 2000))))

    (testing "Limits can be used as map specification keys"
      (is (= {:name "Lucy"
              :friend [{:name "Elizabeth"} {:name "Matthew"}]}
             (d/pull db '[:name {(limit :friend 2) [:name]}] 4))))))

(deftest test-pull-default
  (let [db (create-test-db)]
    (testing "Empty results return nil"
      (is (nil? (d/pull db '[:foo] 1))))

    (testing "A default can be used to replace nil results"
      (is (= {:foo "bar"}
             (d/pull db '[(default :foo "bar")] 1))))))

(deftest test-pull-map
  (let [db (create-test-db)]
    (is (= {:name "Petr" :child [{:name "David"}
                                 {:name "Thomas"}]}
           (d/pull db '[:name {:child [:name]}] 1)))

    (is (= {:name "Matthew" :father {:name "Thomas"}}
           (d/pull db '[:name {:father [:name]}] 6)))

    (is (= {:name "Petr"}
           (d/pull db '[:name {:father [:name]}] 1)))

    ;; Non matching results are removed from collections; even though
    ;; Petr has children, none of those children have a `:foo` attribute
    (is (= {:name "Petr" :child []}
           (d/pull db '[:name {:child [:foo]}] 1)))))

(deftest test-pull-recursion
  (let [db      (-> (create-test-db)
                    (d/db-with [[:db/add 4 :friend 5]
                                [:db/add 5 :friend 6]
                                [:db/add 6 :friend 7]
                                [:db/add 7 :friend 8]
                                [:db/add 4 :enemy 6]
                                [:db/add 5 :enemy 7]
                                [:db/add 6 :enemy 8]
                                [:db/add 7 :enemy 4]]))
        friends {:db/id 4
                 :name "Lucy"
                 :friend
                 [{:db/id 5
                   :name "Elizabeth"
                   :friend
                   [{:db/id 6
                     :name "Matthew"
                     :friend
                     [{:db/id 7
                       :name "Eunan"
                       :friend
                       [{:db/id 8
                         :name "Kerri"}]}]}]}]}
        enemies {:db/id 4 :name "Lucy"
                 :friend
                 [{:db/id 5 :name "Elizabeth"
                   :friend
                   [{:db/id 6 :name "Matthew"
                     :enemy [{:db/id 8 :name "Kerri"}]}]
                   :enemy
                   [{:db/id 7 :name "Eunan"
                     :friend
                     [{:db/id 8 :name "Kerri"}]
                     :enemy
                     [{:db/id 4 :name "Lucy"
                       :friend [{:db/id 5 :name "Elizabeth"}]}]}]}]
                 :enemy
                 [{:db/id 6 :name "Matthew"
                   :friend
                   [{:db/id 7 :name "Eunan"
                     :friend
                     [{:db/id 8 :name "Kerri"}]
                     :enemy [{:db/id 4}]}]
                   :enemy
                   [{:db/id 8 :name "Kerri"}]}]}]

    (testing "Infinite recursion"
      (is (= friends (d/pull db '[:db/id :name {:friend ...}] 4))))

    (testing "Multiple recursion specs in one pattern"
      (is (= enemies (d/pull db '[:db/id :name {:friend 2 :enemy 2}] 4))))

    (let [db (d/db-with db [[:db/add 8 :friend 4]])]
      (testing "Cycles are handled by returning only the :db/id of entities which have been seen before"
        (is (= (update-in friends (take 8 (cycle [:friend 0]))
                          assoc :friend [{:db/id 4}])
               (d/pull db '[:db/id :name {:friend ...}] 4)))))))

(deftest test-deep-recursion
  (let [db    (create-test-db)
        start 100
        depth 10000
        txd   (mapcat
               (fn [idx]
                 [[:db/add idx :name (str "Person-" idx)]
                  [:db/add (dec idx) :friend idx]])
               (range (inc start) depth))
        db    (-> db
                  (d/db-with [[:db/add start :name (str "Person-" start)]])
                  (d/db-with txd))]

    (is (= (str "Person-" (dec depth))
           (->> (d/pull db '[:name {:friend ...}] start)
                (iterate #(get-in % [:friend 0]))
                (drop (dec (- depth start)))
                first
                :name)))))


#_(t/test-ns 'datascript.test.pull)
