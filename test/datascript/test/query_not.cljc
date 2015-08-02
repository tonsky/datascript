(ns datascript.test.query-not
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

(deftest test-not
  (let [db @test-db]
    (are [q res] (= (q/q (concat '[:find ?e :where] (quote q)) db)
                    (into #{} (map vector) res))
      [[?e :name]
       (not [?e :name "Ivan"])]
      #{3 4}
      
      [[?e :name]
       (not
         [?e :name "Ivan"]
         [?e :age  10])]
      #{2 3 4 6}
         
      [[?e :name]
       (not [?e :name "Ivan"])
       (not [?e :age 10])]
      #{4}
    
      ;; full exclude
      [[?e :name]
       (not [?e :age])]
      #{}
         
      ;; not-intersecting rels
      [[?e :name "Ivan"]
       (not [?e :name "Oleg"])]
      #{1 2 5 6}

      ;; exclude empty set
      [[?e :name]
       (not [?e :name "Ivan"]
            [?e :name "Oleg"])]
      #{1 2 3 4 5 6}

      ;; nested excludes
      [[?e :name]
       (not [?e :name "Ivan"]
            (not [?e :age 10]))]
      #{1 3 4 5}
    )))

(deftest test-not-join
  (let [db @test-db]
    (is (= (q/q '[:find ?e ?a
                  :where [?e :name]
                         [?e :age  ?a]
                         (not-join [?e]
                           [?e :name "Oleg"]
                           [?e :age ?a])]
                db)
           #{[1 10] [2 20] [5 10] [6 20]}))
    
    (is (= (q/q '[:find ?e ?a
                  :where [?e :name]
                         [?e :age  ?a]
                         [?e :age  10]
                         (not-join [?e]
                           [?e :name "Oleg"]
                           [?e :age  10]
                           [?e :age ?a])]
                db)
           #{[1 10] [5 10]}))))
  

(deftest test-default-source
  (let [db1 (d/db-with (d/empty-db)
             [ [:db/add 1 :name "Ivan" ]
               [:db/add 2 :name "Oleg"] ])
        db2 (d/db-with (d/empty-db)
             [ [:db/add 1 :age 10 ]
               [:db/add 2 :age 20] ])]
    (are [q res] (= (q/q (concat '[:find ?e :in $ $2 :where] (quote q)) db1 db2)
                    (into #{} (map vector) res))
      ;; NOT inherits default source
      [[?e :name]
       (not [?e :name "Ivan"])]
      #{2}
      
      ;; NOT can reference any source
      [[?e :name]
       (not [$2 ?e :age 10])]
      #{2}
      
      ;; NOT can change default source
      [[?e :name]
       ($2 not [?e :age 10])]
      #{2}
      
      ;; even with another defaul source, it can reference any other source explicitly
      [[?e :name]
       ($2 not [$ ?e :name "Ivan"])]
      #{2}
      
      ;; nested NOT keeps the default source
      [[?e :name]
       ($2 not (not [?e :age 10]))]
      #{1}

      ;; can override nested NOT source
      [[?e :name]
       ($2 not ($ not [?e :name "Ivan"]))]
      #{1})))

(deftest test-impl-edge-cases
  (let [db @test-db]
    (are [q res] (= (q/q (concat '[:find ?e :where] (quote q)) db)
                    (into #{} (map vector) res))
      ;; const \ empty
      [[?e :name "Oleg"]
       [?e :age  10]
       (not [?e :age 20])]
      #{3}
      
      ;; const \ const
      [[?e :name "Oleg"]
       [?e :age  10]
       (not [?e :age 10])]
      #{}
         
      ;; rel \ const
      [[?e :name "Oleg"]
       (not [?e :age 10])]
      #{4})

    ;; 2 rels \ 2 rels
    (is (= (q/q '[:find ?e ?e2
                   :where [?e  :name "Ivan"]
                          [?e2 :name "Ivan"]
                          (not [?e :age 10]
                               [?e2 :age 20])]
                 db)
           #{[2 1] [6 5] [1 1] [2 2] [5 5] [6 6] [2 5] [1 5] [2 6] [6 1] [5 1] [6 2]}))

    ;; 2 rels \ rel + const
    (is (= (q/q '[:find ?e ?e2
                   :where [?e  :name "Ivan"]
                          [?e2 :name "Oleg"]
                          (not [?e :age 10]
                               [?e2 :age 20])]
                 db)
           #{[2 3] [1 3] [2 4] [6 3] [5 3] [6 4]}))

    ;; 2 rels \ 2 consts
    (is (= (q/q '[:find ?e ?e2
                   :where [?e  :name "Oleg"]
                          [?e2 :name "Oleg"]
                          (not [?e :age 10]
                               [?e2 :age 20])]
                 db)
           #{[4 3] [3 3] [4 4]}))
))

(deftest test-insufficient-bindings
  (let [db @test-db]
    (is (thrown-with-msg? ExceptionInfo #"Insufficient bindings: #\{\?e} not bound in \(not \[\?e :name \"Ivan\"]\)"
          (q/q '[:find ?e
                 :where (not [?e :name "Ivan"])
                        [?e :name]]
                db)))
    
    (is (thrown-with-msg? ExceptionInfo #"Insufficient bindings: #\{\?a} not bound in \(not \[1 :age \?a]\)"
          (q/q '[:find ?e
                 :where [?e :name]
                        (not-join [?e]
                          (not [1 :age ?a])
                          [?e :age ?a])]
               db)))
    
    (is (thrown-with-msg? ExceptionInfo #"Insufficient bindings: #\{\?a} not bound in \(not \[\?a :name \"Ivan\"]\)"
          (q/q '[:find ?e
                 :where [?e :name]
                        (not [?a :name "Ivan"])]
               db)))

    (is (thrown-with-msg? ExceptionInfo #"Insufficient bindings: #\{\?f} not bound in \(not \[\?e :age \?f]\)"
          (q/q '[:find ?e
                 :where [?e :name ?a]
                        (not [?e :age ?f])]
               db)))))
