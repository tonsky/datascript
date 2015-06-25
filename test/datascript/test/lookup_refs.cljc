(ns datascript.test.lookup-refs
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.core :as tdc])
    #?(:clj
      (:import [clojure.lang ExceptionInfo])))



(deftest test-lookup-refs
  (let [db (d/db-with (d/empty-db {:name  { :db/unique :db.unique/identity }
                                   :email { :db/unique :db.unique/identity }})
                      [{:db/id 1 :name "Ivan" :email "@1" :age 35}
                       {:db/id 2 :name "Petr" :email "@2" :age 22}])]
    
    (are [eid res] (= (tdc/entity-map db eid) res)
      [:name "Ivan"]   {:db/id 1 :name "Ivan" :email "@1" :age 35}
      [:email "@1"]    {:db/id 1 :name "Ivan" :email "@1" :age 35}
      [:name "Sergey"] nil)
    
    (are [eid msg] (thrown-with-msg? ExceptionInfo msg (d/entity db eid))
      [:name]     #"Lookup ref should contain 2 elements"
      [:name 1 2] #"Lookup ref should contain 2 elements"
      [:age 10]   #"Lookup ref attribute should be marked as :db.unique/identity")))

(deftest test-lookup-refs-transact
  (let [db (d/db-with (d/empty-db {:name    { :db/unique :db.unique/identity }
                                   :friend  { :db/valueType :db.type/ref }})
                      [{:db/id 1 :name "Ivan"}
                       {:db/id 2 :name "Petr"}])]
    (are [tx res] (= (tdc/entity-map (d/db-with db tx) 1) res)
      ;; Additions
      [[:db/add [:name "Ivan"] :age 35]]
      {:db/id 1 :name "Ivan" :age 35}
      
      [{:db/id [:name "Ivan"] :age 35}]
      {:db/id 1 :name "Ivan" :age 35}
         
      [[:db/add 1 :friend [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}

      [[:db/add 1 :friend [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}
         
      [{:db/id 1 :friend [:name "Petr"]}]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}
      
      [{:db/id 2 :_friend [:name "Ivan"]}]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}
      
      ;; lookup refs are resolved at intermediate DB value
      [[:db/add 3 :name "Oleg"]
       [:db/add 1 :friend [:name "Oleg"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 3}}
      
      ;; CAS
      [[:db.fn/cas [:name "Ivan"] :name "Ivan" "Oleg"]]
      {:db/id 1 :name "Oleg"}
      
      [[:db/add 1 :friend 1]
       [:db.fn/cas 1 :friend [:name "Ivan"] 2]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}
         
      [[:db/add 1 :friend 1]
       [:db.fn/cas 1 :friend 1 [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}
         
      ;; Retractions
      [[:db/add 1 :age 35]
       [:db/retract [:name "Ivan"] :age 35]]
      {:db/id 1 :name "Ivan"}
      
      [[:db/add 1 :friend 2]
       [:db/retract 1 :friend [:name "Petr"]]]
      {:db/id 1 :name "Ivan"}
         
      [[:db/add 1 :age 35]
       [:db.fn/retractAttribute [:name "Ivan"] :age]]
      {:db/id 1 :name "Ivan"}
         
      [[:db.fn/retractEntity [:name "Ivan"]]]
      {:db/id 1}
    )))

(deftest test-lookup-refs-transact-multi
  (let [db (d/db-with (d/empty-db {:name    { :db/unique :db.unique/identity }
                                   :friends { :db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/many }})
                      [{:db/id 1 :name "Ivan"}
                       {:db/id 2 :name "Petr"}
                       {:db/id 3 :name "Oleg"}
                       {:db/id 4 :name "Sergey"}])]
    (are [tx res] (= (tdc/entity-map (d/db-with db tx) 1) res)
      ;; Additions
      [[:db/add 1 :friends [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [[:db/add 1 :friends [:name "Petr"]]
       [:db/add 1 :friends [:name "Oleg"]]]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}}
         
      [{:db/id 1 :friends [:name "Petr"]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 1 :friends [[:name "Petr"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}
         
      [{:db/id 1 :friends [[:name "Petr"] [:name "Oleg"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}}

      [{:db/id 1 :friends [2 [:name "Oleg"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}}

      [{:db/id 1 :friends [[:name "Petr"] 3]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}}
         
      ;; reverse refs
      [{:db/id 2 :_friends [:name "Ivan"]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 2 :_friends [[:name "Ivan"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 2 :_friends [[:name "Ivan"] [:name "Oleg"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}
    )))

(deftest lookup-refs-index-access
  (let [db (d/db-with (d/empty-db {:name    { :db/unique :db.unique/identity }
                                   :friends { :db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/many}})
                      [{:db/id 1 :name "Ivan" :friends [2 3]}
                       {:db/id 2 :name "Petr" :friends 3}
                       {:db/id 3 :name "Oleg"}])]
     (are [index attrs datoms] (= (map (juxt :e :a :v) (apply d/datoms db index attrs)) datoms)
       :eavt [[:name "Ivan"]]
       [[1 :friends 2] [1 :friends 3] [1 :name "Ivan"]]
       
       :eavt [[:name "Ivan"] :friends]
       [[1 :friends 2] [1 :friends 3]]
          
       :eavt [[:name "Ivan"] :friends [:name "Petr"]]
       [[1 :friends 2]]
       
       :aevt [:friends [:name "Ivan"]]
       [[1 :friends 2] [1 :friends 3]]
          
       :aevt [:friends [:name "Ivan"] [:name "Petr"]]
       [[1 :friends 2]]
       
       :avet [:friends [:name "Oleg"]]
       [[1 :friends 3] [2 :friends 3]]
       
       :avet [:friends [:name "Oleg"] [:name "Ivan"]]
       [[1 :friends 3]])
    
     (are [index attrs resolved-attrs] (= (vec (apply d/seek-datoms db index attrs))
                                          (vec (apply d/seek-datoms db index resolved-attrs)))
       :eavt [[:name "Ivan"]] [1]
       :eavt [[:name "Ivan"] :name] [1 :name]
       :eavt [[:name "Ivan"] :friends [:name "Oleg"]] [1 :friends 3]
       
       :aevt [:friends [:name "Petr"]] [:friends 2]
       :aevt [:friends [:name "Ivan"] [:name "Oleg"]] [:friends 1 3]
       
       :avet [:friends [:name "Oleg"]] [:friends 3]
       :avet [:friends [:name "Oleg"] [:name "Petr"]] [:friends 3 2]
      )
    
    (are [attr start end datoms] (= (map (juxt :e :a :v) (d/index-range db attr start end)) datoms)
       :friends [:name "Oleg"] [:name "Oleg"]
       [[1 :friends 3] [2 :friends 3]]
       
       :friends [:name "Petr"] [:name "Petr"]
       [[1 :friends 2]]
       
       :friends [:name "Petr"] [:name "Oleg"]
       [[1 :friends 2] [1 :friends 3] [2 :friends 3]])
))

(deftest test-lookup-refs-query
  (let [schema {:name   { :db/unique :db.unique/identity }
                :friend { :db/valueType :db.type/ref }}
        db (d/db-with (d/empty-db schema)
                    [{:db/id 1 :id 1 :name "Ivan" :age 11 :friend 2}
                     {:db/id 2 :id 2 :name "Petr" :age 22 :friend 3}
                     {:db/id 3 :id 3 :name "Oleg" :age 33 }])]
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?e
                       :where [?e :age ?v]]
                     db [:name "Ivan"]))
           #{11}))
    
    (is (= (set (d/q '[:find [?v ...]
                       :in $ [?e ...]
                       :where [?e :age ?v]]
                     db [[:name "Ivan"] [:name "Petr"]]))
           #{11 22}))
    
    (is (= (set (d/q '[:find [?e ...]
                       :in $ ?v
                       :where [?e :friend ?v]]
                     db [:name "Petr"]))
           #{1}))
    
    (is (= (set (d/q '[:find [?e ...]
                       :in $ [?v ...]
                       :where [?e :friend ?v]]
                     db [[:name "Petr"] [:name "Oleg"]]))
           #{1 2}))
    
    (is (= (d/q '[:find ?e ?v
                  :in $ ?e ?v
                  :where [?e :friend ?v]]
                db [:name "Ivan"] [:name "Petr"])
           #{[[:name "Ivan"] [:name "Petr"]]}))
    
    (is (= (d/q '[:find ?e ?v
                  :in $ [?e ...] [?v ...]
                  :where [?e :friend ?v]]
                db [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]]
                   [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]])
           #{[[:name "Ivan"] [:name "Petr"]]
             [[:name "Petr"] [:name "Oleg"]]}))
    
    (let [db2 (d/db-with (d/empty-db schema)
                [{:db/id 3 :name "Ivan" :id 3}
                 {:db/id 1 :name "Petr" :id 1}
                 {:db/id 2 :name "Oleg" :id 2}])]
      (is (= (d/q '[:find ?e ?e1 ?e2
                    :in $1 $2 [?e ...]
                    :where [$1 ?e :id ?e1]
                           [$2 ?e :id ?e2]]
                  db db2 [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]])
             #{[[:name "Ivan"] 1 3]
               [[:name "Petr"] 2 1]
               [[:name "Oleg"] 3 2]})))
    
    (testing "inline refs"
      (is (= (d/q '[:find ?v
                    :where [[:name "Ivan"] :friend ?v]]
                  db)
             #{[2]}))
      
      (is (= (d/q '[:find ?e
                    :where [?e :friend [:name "Petr"]]]
                  db)
             #{[1]}))
      
      (is (thrown-with-msg? ExceptionInfo #"Nothing found"
            (d/q '[:find ?e
                   :where [[:name "Valery"] :friend ?e]]
                  db)))

      )
))

#_(test-lookup-refs-query)
