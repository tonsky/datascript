(ns datascript.test.index
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

(deftest test-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db { :age  { :db/index true
                                     :db/order 0}
                            :name {:db/order 1}})
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11] ]))]
    (testing "Main indexes, sort order"
      (is (= (map dvec (d/datoms db :aevt))
             [ [1 0 44]
               [2 0 25]
               [3 0 11]
               [1 1 "Petr"]
               [2 1 "Ivan"]
               [3 1 "Sergey"] ]))

      (is (= (map dvec (d/datoms db :eavt))
             [ [1 0 44]
               [1 1 "Petr"]
               [2 0 25]
               [2 1 "Ivan"]
               [3 0 11]
               [3 1 "Sergey"] ]))

      (is (= (map dvec (d/datoms db :avet))
             [ [3 0 11]
               [2 0 25]
               [1 0 44] ]))) ;; name non-indexed, excluded from avet

    (testing "Components filtration"
      (is (= (map dvec (d/datoms db :eavt 1))
             [ [1 0 44]
               [1 1 "Petr"] ]))

      (is (= (map dvec (d/datoms db :eavt 1 :age))
             [ [1 0 44] ]))

      (is (= (map dvec (d/datoms db :avet :age))
             [ [3 0 11]
               [2 0 25]
               [1 0 44] ])))))

(deftest test-seek-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db { :name { :db/index true
                                     :db/order 1}
                             :age  { :db/index true
                                     :db/order 0} })
               (d/db-with [[:db/add 1 :name "Petr"]
                           [:db/add 1 :age 44]
                           [:db/add 2 :name "Ivan"]
                           [:db/add 2 :age 25]
                           [:db/add 3 :name "Sergey"]
                           [:db/add 3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/seek-datoms db :avet :age 10))
             [ [3 0 11]
               [2 0 25]
               [1 0 44]
               [2 1 "Ivan"]
               [1 1 "Petr"]
               [3 1 "Sergey"] ])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "P"))
             [ [1 1 "Petr"]
               [3 1 "Sergey"] ])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "Petr"))
             [ [1 1 "Petr"]
               [3 1 "Sergey"] ])))))

(deftest test-rseek-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db {:age  {:db/index true
                                   :db/order 0}
                            :name {:db/index true
                                   :db/order 1}})
               (d/db-with [[:db/add 1 :name "Petr"]
                           [:db/add 1 :age 44]
                           [:db/add 2 :name "Ivan"]
                           [:db/add 2 :age 25]
                           [:db/add 3 :name "Sergey"]
                           [:db/add 3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/rseek-datoms db :avet :name "Petr"))
             [ [1 1 "Petr"]
               [2 1 "Ivan"]
               [1 0 44]
               [2 0 25]
               [3 0 11]])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/rseek-datoms db :avet :age 26))
             [ [2 0 25]
               [3 0 11]])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/rseek-datoms db :avet :age 25))
             [ [2 0 25]
               [3 0 11]])))))

(deftest test-index-range
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db    (d/db-with
                (d/empty-db { :name { :db/index true
                                     :db/order 1}
                              :age  { :db/index true
                                     :db/order 0} })
                [ { :db/id 1 :name "Ivan"   :age 15 }
                  { :db/id 2 :name "Oleg"   :age 20 }
                  { :db/id 3 :name "Sergey" :age 7 }
                  { :db/id 4 :name "Pavel"  :age 45 }
                  { :db/id 5 :name "Petr"   :age 20 } ])]
    (is (= (map dvec (d/index-range db :name "Pe" "S"))
           [ [5 1 "Petr"] ]))
    (is (= (map dvec (d/index-range db :name "O" "Sergey"))
           [ [2 1 "Oleg"]
             [4 1 "Pavel"]
             [5 1 "Petr"]
             [3 1 "Sergey"] ]))

    (is (= (map dvec (d/index-range db :name nil "P"))
           [ [1 1 "Ivan"]
             [2 1 "Oleg"] ]))
    (is (= (map dvec (d/index-range db :name "R" nil))
           [ [3 1 "Sergey"] ]))
    (is (= (map dvec (d/index-range db :name nil nil))
           [ [1 1 "Ivan"]
             [2 1 "Oleg"]
             [4 1 "Pavel"]
             [5 1 "Petr"]
             [3 1 "Sergey"] ]))

    (is (= (map dvec (d/index-range db :age 15 20))
           [ [1 0 15]
             [2 0 20]
             [5 0 20]]))
    (is (= (map dvec (d/index-range db :age 7 45))
           [ [3 0 7]
             [1 0 15]
             [2 0 20]
             [5 0 20]
             [4 0 45] ]))
    (is (= (map dvec (d/index-range db :age 0 100))
           [ [3 0 7]
             [1 0 15]
             [2 0 20]
             [5 0 20]
             [4 0 45] ]))))
