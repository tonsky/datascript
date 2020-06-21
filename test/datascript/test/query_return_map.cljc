(ns datascript.test.query-return-map
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

(def test-db
  (d/db-with (d/empty-db)
    [[:db/add 1 :name "Petr"]
     [:db/add 1 :age 44]
     [:db/add 2 :name "Ivan"]
     [:db/add 2 :age 25]
     [:db/add 3 :name "Sergey"]
     [:db/add 3 :age 11]]))

(deftest test-find-specs
  (is (= (d/q '[:find ?name ?age
                :keys n a
                :where [?e :name ?name]
                       [?e :age  ?age]]
           test-db)
        #{{:n "Petr" :a 44} {:n "Ivan" :a 25} {:n "Sergey" :a 11}}))
  (is (= (d/q '[:find ?name ?age
                :syms n a
                :where [?e :name ?name]
                       [?e :age  ?age]]
           test-db)
        #{{'n "Petr" 'a 44} {'n "Ivan" 'a 25} {'n "Sergey" 'a 11}}))
  (is (= (d/q '[:find ?name ?age
                :strs n a
                :where [?e :name ?name]
                       [?e :age  ?age]]
           test-db)
        #{{"n" "Petr" "a" 44} {"n" "Ivan" "a" 25} {"n" "Sergey" "a" 11}}))

  (is (= (d/q '[:find [?name ?age]
                :keys n a
                :where [?e :name ?name]
                       [(= ?name "Ivan")]
                       [?e :age  ?age]]
           test-db)
        {:n "Ivan" :a 25})))


