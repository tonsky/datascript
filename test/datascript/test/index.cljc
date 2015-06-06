(ns datascript.test.index
  (:require
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [is are deftest testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.test.core :as tdc]))

(deftest test-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db)
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11] ]))]
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
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db)
               (d/db-with [[:db/add 1 :name "Petr"]
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

(deftest test-index-range
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db    (d/db-with
                (d/empty-db)
                [ { :db/id 1 :name "Ivan"   :age 15 }
                  { :db/id 2 :name "Oleg"   :age 20 }
                  { :db/id 3 :name "Sergey" :age 7 }
                  { :db/id 4 :name "Pavel"  :age 45 }
                  { :db/id 5 :name "Petr"   :age 20 } ])]
    (is (= (map dvec (d/index-range db :name "Pe" "S"))
           [ [5 :name "Petr"] ]))
    (is (= (map dvec (d/index-range db :name "O" "Sergey"))
           [ [2 :name "Oleg"]
             [4 :name "Pavel"]
             [5 :name "Petr"]
             [3 :name "Sergey"] ]))

    (is (= (map dvec (d/index-range db :name nil "P"))
           [ [1 :name "Ivan"]
             [2 :name "Oleg"] ]))
    (is (= (map dvec (d/index-range db :name "R" nil))
           [ [3 :name "Sergey"] ]))
    (is (= (map dvec (d/index-range db :name nil nil))
           [ [1 :name "Ivan"]
             [2 :name "Oleg"]
             [4 :name "Pavel"]
             [5 :name "Petr"]
             [3 :name "Sergey"] ]))

    (is (= (map dvec (d/index-range db :age 15 20))
           [ [1 :age 15]
             [2 :age 20]
             [5 :age 20]]))
    (is (= (map dvec (d/index-range db :age 7 45))
           [ [3 :age 7]
             [1 :age 15]
             [2 :age 20]
             [5 :age 20]
             [4 :age 45] ]))
    (is (= (map dvec (d/index-range db :age 0 100))
           [ [3 :age 7]
             [1 :age 15]
             [2 :age 20]
             [5 :age 20]
             [4 :age 45] ]))))
