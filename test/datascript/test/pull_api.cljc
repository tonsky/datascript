(ns datascript.test.pull-api
  (:require
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [is are deftest testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.test.core :as tdc]))

(def ^:private test-schema
  {:aka    { :db/cardinality :db.cardinality/many }
   :child  { :db/cardinality :db.cardinality/many
             :db/valueType :db.type/ref }
   :friend { :db/cardinality :db.cardinality/many
             :db/valueType :db.type/ref }
   :enemy  { :db/cardinality :db.cardinality/many
             :db/valueType :db.type/ref }
   :father { :db/valueType :db.type/ref }

   :part   { :db/valueType :db.type/ref
             :db/isComponent true
             :db/cardinality :db.cardinality/many }
   :spec   { :db/valueType :db.type/ref
             :db/isComponent true
             :db/cardinality :db.cardinality/one }})

(def test-datoms
  (->>
    [[1 :name  "Petr"]
     [1 :aka   "Devil"]
     [1 :aka   "Tupen"]
     [2 :name  "David"]
     [3 :name  "Thomas"]
     [4 :name  "Lucy"]
     [5 :name  "Elizabeth"]
     [6 :name  "Matthew"]
     [7 :name  "Eunan"]
     [8 :name  "Kerri"]
     [9 :name  "Rebecca"]
     [1 :child 2]
     [1 :child 3]
     [6 :father 3]
     [10 :name  "Part A"]
     [11 :name  "Part A.A"]
     [10 :part 11]
     [12 :name  "Part A.A.A"]
     [11 :part 12]
     [13 :name  "Part A.A.A.A"]
     [12 :part 13]
     [14 :name  "Part A.A.A.B"]
     [12 :part 14]
     [15 :name  "Part A.B"]
     [10 :part 15]
     [16 :name  "Part A.B.A"]
     [15 :part 16]
     [17 :name  "Part A.B.A.A"]
     [16 :part 17]
     [18 :name  "Part A.B.A.B"]
     [16 :part 18]]
   (map #(apply d/datom %))))

(def ^:private test-db (d/init-db test-datoms test-schema))

(deftest test-pull-attr-spec
  (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
         (d/pull test-db '[:name :aka] 1)))

  (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
         (d/pull test-db '[:name :father :db/id] 6)))

  (is (= [{:name "Petr"} {:name "Elizabeth"}
          {:name "Eunan"} {:name "Rebecca"}]
         (d/pull-many test-db '[:name] [1 5 7 9]))))

(deftest test-pull-reverse-attr-spec
  (is (= {:name "David" :_child [{:db/id 1}]}
         (d/pull test-db '[:name :_child] 2)))

  (is (= {:name "David" :_child [{:name "Petr"}]}
         (d/pull test-db '[:name {:_child [:name]}] 2))))

(deftest test-pull-component-attr
  (let [parts {:name "Part A",
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
                    {:db/id 18 :name "Part A.B.A.B"}]}]}]}
        rpart (update-in parts [:part 0 :part 0 :part]
                         (partial into [{:db/id 10}]))
        recdb (d/init-db
               (concat test-datoms [(d/datom 12 :part 10)])
               test-schema)

        mutdb (d/init-db
               (concat test-datoms [(d/datom 12 :part 10)
                                    (d/datom 12 :spec 10)
                                    (d/datom 10 :spec 13)
                                    (d/datom 13 :spec 12)])
               test-schema)]
    
    (testing "Component entities are expanded recursively"
      (is (= parts (d/pull test-db '[:name :part] 10))))

    (testing "Reverse component references yield a single result"
      (is (= {:name "Part A.A" :_part {:db/id 10}}
             (d/pull test-db [:name :_part] 11))))
    
    (testing "Like explicit recursion, expansion will not allow loops"
      (is (= rpart (d/pull recdb '[:name :part] 10))))))

(deftest test-pull-wildcard
  (is (= {:db/id 1 :name "Petr" :aka ["Devil" "Tupen"]
          :child [{:db/id 2} {:db/id 3}]}
         (d/pull test-db '[*] 1)))

  (is (= {:db/id 2 :name "David" :_child [{:db/id 1}]}
         (d/pull test-db '[* :_child] 2))))

(deftest test-pull-limit
  (let [db (d/init-db
             (concat
               test-datoms
               [(d/datom 4 :friend 5)
                (d/datom 4 :friend 6)
                (d/datom 4 :friend 7)
                (d/datom 4 :friend 8)]
               (for [idx (range 2000)]
                 (d/datom 8 :aka (str "aka-" idx))))
              test-schema)]

    (testing "Without an explicit limit, the default is 1000"
      (is (= 1000 (->> (d/pull db '[:aka] 8) :aka count))))

    (testing "Explicit limit can reduce the default"
      (is (= 500 (->> (d/pull db '[(limit :aka 500)] 8) :aka count))))

    (testing "Explicit limit can increase the default"
      (is (= 1500 (->> (d/pull db '[(limit :aka 1500)] 8) :aka count))))

    (testing "A nil limit produces unlimited results"
      (is (= 2000 (->> (d/pull db '[(limit :aka nil)] 8) :aka count))))

    (testing "Limits can be used as map specification keys"
      (is (= {:name "Lucy"
              :friend [{:name "Elizabeth"} {:name "Matthew"}]}
             (d/pull db '[:name {(limit :friend 2) [:name]}] 4))))))

(deftest test-pull-default
  (testing "Empty results return nil"
    (is (nil? (d/pull test-db '[:foo] 1))))

  (testing "A default can be used to replace nil results"
    (is (= {:foo "bar"}
           (d/pull test-db '[(default :foo "bar")] 1)))))

(deftest test-pull-map
  (testing "Single attrs yield a map"
    (is (= {:name "Matthew" :father {:name "Thomas"}}
           (d/pull test-db '[:name {:father [:name]}] 6))))

  (testing "Multi attrs yield a collection of maps"
    (is (= {:name "Petr" :child [{:name "David"}
                                 {:name "Thomas"}]}
           (d/pull test-db '[:name {:child [:name]}] 1))))

  (testing "Missing attrs are dropped"
    (is (= {:name "Petr"}
           (d/pull test-db '[:name {:father [:name]}] 1))))

  (testing "Non matching results are removed from collections"
    (is (= {:name "Petr" :child []}
           (d/pull test-db '[:name {:child [:foo]}] 1))))

  (testing "Map specs can override component expansion"
    (let [parts {:name "Part A" :part [{:name "Part A.A"} {:name "Part A.B"}]}]
      (is (= parts
             (d/pull test-db '[:name {:part [:name]}] 10)))

      (is (= parts
             (d/pull test-db '[:name {:part 1}] 10))))))

(deftest test-pull-recursion
  (let [db      (-> test-db
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
                       :friend [{:db/id 5}]}]}]}]
                 :enemy
                 [{:db/id 6 :name "Matthew"
                   :friend
                   [{:db/id 7 :name "Eunan"
                     :friend
                     [{:db/id 8 :name "Kerri"}]
                     :enemy [{:db/id 4 :name "Lucy"
                              :friend [{:db/id 5 :name "Elizabeth"}]}]}]
                   :enemy
                   [{:db/id 8 :name "Kerri"}]}]}]

    (testing "Infinite recursion"
      (is (= friends (d/pull db '[:db/id :name {:friend ...}] 4))))

    (testing "Multiple recursion specs in one pattern"
      (is (= enemies (d/pull db '[:db/id :name {:friend 2 :enemy 2}] 4))))

    (let [db (d/db-with db [[:db/add 8 :friend 4]])]
      (testing "Cycles are handled by returning only the :db/id of entities which have been seen before"
        (is (= (update-in friends (take 8 (cycle [:friend 0]))
                          assoc :friend [{:db/id 4 :name "Lucy" :friend [{:db/id 5}]}])
               (d/pull db '[:db/id :name {:friend ...}] 4)))))))

(deftest test-dual-recursion
  (let [empty (d/empty-db {:part { :db/valueType :db.type/ref }
                           :spec { :db/valueType :db.type/ref }})]
    (let [db (d/db-with empty [[:db/add 1 :part 2]
                               [:db/add 2 :part 3]
                               [:db/add 3 :part 1]
                               [:db/add 1 :spec 2]
                               [:db/add 2 :spec 1]])]
      (is (= (d/pull db '[:db/id {:part ...} {:spec ...}] 1)
             {:db/id 1,
              :spec {:db/id 2
                     :spec {:db/id 1,
                            :spec {:db/id 2}, :part {:db/id 2}}
                     :part {:db/id 3,
                            :part {:db/id 1,
                                   :spec {:db/id 2},
                                   :part {:db/id 2}}}}
              :part {:db/id 2
                     :spec {:db/id 1, :spec {:db/id 2}, :part {:db/id 2}}
                     :part {:db/id 3,
                            :part {:db/id 1,
                                   :spec {:db/id 2},
                                   :part {:db/id 2}}}}})))))

(deftest test-deep-recursion
  (let [start 100
        depth 1500
        txd   (mapcat
               (fn [idx]
                 [(d/datom idx :name (str "Person-" idx))
                  (d/datom (dec idx) :friend idx)])
               (range (inc start) depth))
        db    (d/init-db (concat
                           test-datoms
                           [(d/datom start :name (str "Person-" start))]
                           txd)
                         test-schema)
        pulled (d/pull db '[:name {:friend ...}] start)
        path   (->> [:friend 0]
                    (repeat (dec (- depth start)))
                    (into [] cat))]
    (is (= (str "Person-" (dec depth))
           (:name (get-in pulled path))))))

#_(t/test-ns 'datascript.test.pull-api)
