(ns datascript.test.query-or
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.query-v3 :as q]
    [datascript.test.core :as tdc])
    #?(:clj
      (:import [clojure.lang ExceptionInfo])))

(def test-db
  (delay
    (d/db-with (d/empty-db)
      [ {:db/id 1 :name "Ivan" :age 10}
        {:db/id 2 :name "Ivan" :age 20}
        {:db/id 3 :name "Oleg" :age 10}
        {:db/id 4 :name "Oleg" :age 20}
        {:db/id 5 :name "Ivan" :age 10}
        {:db/id 6 :name "Ivan" :age 20} ])))

(deftest test-or
  (let [db @test-db]
    (are [q res] (= (q/q (concat '[:find ?e :where] (quote q)) db)
                    (into #{} (map vector) res))

      ;; intersecting results
      [(or [?e :name "Oleg"]
           [?e :age 10])]
      #{1 3 4 5}
         
      ;; one branch empty
      [(or [?e :name "Oleg"]
           [?e :age 30])]
      #{3 4}
        
      ;; both empty
      [(or [?e :name "Petr"]
           [?e :age 30])]
      #{}
         
      ;; join with 1 var
      [[?e :name "Ivan"]
       (or [?e :name "Oleg"]
           [?e :age 10])]
      #{1 5}
      
      ;; join with 2 vars
      [[?e :age ?a]
       (or (and [?e :name "Ivan"]
                [1  :age  ?a])
           (and [?e :name "Oleg"]
                [2  :age  ?a]))]
      #{1 5 4}
)))

(deftest test-or-join
  (let [db @test-db]
    (are [q res] (= (q/q (concat '[:find ?e :where] (quote q)) db)
                    (into #{} (map vector) res))
      [(or-join [?e]
         [?e :name ?n]
         (and [?e :age ?a]
              [?e :name ?n]))]
      #{1 2 3 4 5 6}
         
      [[?e  :name ?a]
       [?e2 :name ?a]
       (or-join [?e]
         (and [?e  :age ?a]
              [?e2 :age ?a]))]
      #{1 2 3 4 5 6})))

(deftest test-default-source
  (let [db1 (d/db-with (d/empty-db)
             [ [:db/add 1 :name "Ivan" ]
               [:db/add 2 :name "Oleg"] ])
        db2 (d/db-with (d/empty-db)
             [ [:db/add 1 :age 10 ]
               [:db/add 2 :age 20] ])]
    (are [q res] (= (q/q (concat '[:find ?e :in $ $2 :where] (quote q)) db1 db2)
                    (into #{} (map vector) res))
      ;; OR inherits default source
      [[?e :name]
       (or [?e :name "Ivan"])]
      #{1}
      
      ;; OR can reference any source
      [[?e :name]
       (or [$2 ?e :age 10])]
      #{1}
      
      ;; OR can change default source
      [[?e :name]
       ($2 or [?e :age 10])]
      #{1}
      
      ;; even with another defaul source, it can reference any other source explicitly
      [[?e :name]
       ($2 or [$ ?e :name "Ivan"])]
      #{1}
      
      ;; nested OR keeps the default source
      [[?e :name]
       ($2 or (or [?e :age 10]))]
      #{1}

      ;; can override nested OR source
      [[?e :name]
       ($2 or ($ or [?e :name "Ivan"]))]
      #{1})))

(deftest test-insufficient-bindings
  (let [db @test-db]
    (is (thrown-with-msg? ExceptionInfo #"Insufficient bindings: #\{\?e} not bound in \(or-join \[\[\?e]] \[\?e :name \"Ivan\"]\)"
          (q/q '[:find ?e
                 :where (or-join [[?e]]
                          [?e :name "Ivan"])] db)))))
