(ns datascript.test.transact
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-with
  (let [db  (-> (d/empty-db {:aka { :db/cardinality :db.cardinality/many
                                    :db/order 0}
                             :name {:db/order 1}})
                (d/db-with [[:db/add 1 :name "Ivan"]])
                (d/db-with [[:db/add 1 :name "Petr"]])
                (d/db-with [[:db/add 1 :aka  "Devil"]])
                (d/db-with [[:db/add 1 :aka  "Tupen"]]))]

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] db)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] db)
           #{["Devil"] ["Tupen"]}))

    (testing "Retract"
      (let [db  (-> db
                  (d/db-with [[:db/retract 1 :name "Petr"]])
                  (d/db-with [[:db/retract 1 :aka  "Devil"]]))]

        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{}))
        (is (= (d/q '[:find ?v
                      :where [1 :aka ?v]] db)
               #{["Tupen"]}))

        (is (= (into {} (d/entity db 1)) { :aka #{"Tupen"} }))))

    (testing "Cannot retract what's not there"
      (let [db  (-> db
                    (d/db-with [[:db/retract 1 :name "Ivan"]]))]
        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{["Petr"]})))))
  
  (testing "Skipping nils in tx"
    (let [db (-> (d/empty-db { :attr {:db/order 0}})
                 (d/db-with [[:db/add 1 :attr 2]
                             nil
                             [:db/add 3 :attr 4]]))]
      (is (= [[1 0 2], [3 0 4]]
             (map (juxt :e :a :v) (d/datoms db :eavt)))))))


(deftest test-with-datoms
  (testing "keeps tx number"
    (let [db (-> (d/empty-db {:name {:db/order 0}
                              :age {:db/order 1}
                              :aka {:db/order 2}})
                 (d/db-with [(d/datom 1 0 "Oleg")
                             (d/datom 1 1  17 (- d/tx0 1))
                             [:db/add 1 :aka  "x" (- d/tx0 2)]]))]
      (is (= [[1 0 "Oleg" d/tx0      ]
              [1 1  17     (- d/tx0 1)]
              [1 2  "x"    (- d/tx0 2)]]
             (map (juxt :e :a :v :tx)
                  (d/datoms db :eavt))))))
  
  (testing "retraction"
    (let [db (-> (d/empty-db {:name {:db/order 0}
                              :age {:db/order 1}})
                 (d/db-with [(d/datom 1 0 "Oleg")
                             (d/datom 1 1  17)
                             (d/datom 1 0 "Oleg" d/tx0 false)]))]
      (is (= [[1 1 17 d/tx0]]
             (map (juxt :e :a :v :tx)
                  (d/datoms db :eavt)))))))

(deftest test-retract-fns
  (let [db (-> (d/empty-db {:aka    { :db/cardinality :db.cardinality/many
                                      :db/order 0}
                            :friend { :db/valueType :db.type/ref
                                      :db/order 1}
                            :name {:db/order 3}
                            :age {:db/order 4}})
               (d/db-with [ { :db/id 1, :name  "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2 }
                            { :db/id 2, :name  "Petr", :age 37 } ]))]
    (let [db (d/db-with db [ [:db.fn/retractEntity 1] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[3 "Petr"] [4 37]})))

    (testing "Retract entitiy with incoming refs"
      (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
             #{[2]}))
      
      (let [db (d/db-with db [ [:db.fn/retractEntity 2] ])]
        (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
               #{}))))
    
    (let [db (d/db-with db [ [:db.fn/retractAttribute 1 :name] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[4 15] [0 "X"] [0 "Y"] [0 "Z"] [1 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[3 "Petr"] [4 37]})))

    (let [db (d/db-with db [ [:db.fn/retractAttribute 1 :aka] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[3 "Ivan"] [4 15] [1 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[3 "Petr"] [4 37]})))))

(deftest test-retract-fns-not-found
  (let [db  (-> (d/empty-db { :name { :db/unique :db.unique/identity
                                      :db/order 0} })
                (d/db-with  [[:db/add 1 :name "Ivan"]]))
        all #(vec (d/datoms % :eavt))]
    (are [op] (= [(d/datom 1 0 "Ivan")]
                 (all (d/db-with db [op])))
      [:db/retract             2 :name "Petr"]
      [:db.fn/retractAttribute 2 :name]
      [:db.fn/retractEntity    2]
      [:db/retract             [:name "Petr"] :name "Petr"]
      [:db.fn/retractAttribute [:name "Petr"] :name]
      [:db.fn/retractEntity    [:name "Petr"]])
         
    (are [op] (= [[] []] 
                 [(all (d/db-with db [op]))
                  (all (d/db-with db [op op]))]) ;; idempotency
      [:db/retract             1 :name "Ivan"]
      [:db.fn/retractAttribute 1 :name]
      [:db.fn/retractEntity    1]
      [:db/retract             [:name "Ivan"] :name "Ivan"]
      [:db.fn/retractAttribute [:name "Ivan"] :name]
      [:db.fn/retractEntity    [:name "Ivan"]])))

(deftest test-transact!
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many
                                    :db/order 0}
                             :name {:db/order 1}})]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]])

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))))

(deftest test-db-fn-cas
  (let [conn (d/create-conn {:weight {:db/order 0}
                             :label {:db/order 1}})]
    (d/transact! conn [[:db/add 1 :weight 200]])
    (d/transact! conn [[:db.fn/cas 1 :weight 200 300]])
    (is (= (:weight (d/entity @conn 1)) 300))
    (is (thrown-with-msg? Throwable #":db.fn/cas failed on datom \[1 :weight 300\], expected 200"
                          (d/transact! conn [[:db.fn/cas 1 :weight 200 210]]))))
  
  (let [conn (d/create-conn {:label { :db/cardinality :db.cardinality/many
                                      :db/order 0}})]
    (d/transact! conn [[:db/add 1 :label :x]])
    (d/transact! conn [[:db/add 1 :label :y]])
    (d/transact! conn [[:db.fn/cas 1 :label :y :z]])
    (is (= (:label (d/entity @conn 1)) #{:x :y :z}))
    (is (thrown-with-msg? Throwable #":db.fn/cas failed on datom \[1 :label \(:x :y :z\)\], expected :s"
                          (d/transact! conn [[:db.fn/cas 1 :label :s :t]]))))

  (let [conn (d/create-conn {:name {:db/order 0}
                             :age {:db/order 1}})]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db.fn/cas 1 :age nil 42]])
    (is (= (:age (d/entity @conn 1)) 42))
    (is (thrown-with-msg? Throwable #":db.fn/cas failed on datom \[1 :age 42\], expected nil"
                          (d/transact! conn [[:db.fn/cas 1 :age nil 4711]])))))

(deftest test-db-fn
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many
                                    :db/order 0}
                             :age {:db/order 1}
                             :name {:db/order 2}
                             :had-birthday {:db/order 3}})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                  db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid :had-birthday true]]
                    (throw (new Throwable (str "No entity with name: " name)))))]
    (d/transact! conn [{:db/id 1 :name "Ivan" :age 31}])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]])
    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                         [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-with-msg? Throwable #"No entity with name: Bob"
                          (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
          e (d/entity db-after 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))

(deftest test-resolve-eid
  (let [conn (d/create-conn {:name {:db/order 0}
                             :age  {:db/order 1}})
        t1   (d/transact! conn [[:db/add -1 :name "Ivan"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Petr"]
                                [:db/add -2 :age 22]])
        t2   (d/transact! conn [[:db/add "Serg" :name "Sergey"]
                                [:db/add "Serg" :age 30]])]
    (is (= (:tempids t1) { -1 1, -2 2, :db/current-tx (- d/tx0 1) }))
    (is (= (:tempids t2) { "Serg" 3, :db/current-tx (- d/tx0 2) }))
    (is (= (d/q '[:find  ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                         [?e :age ?a]] @conn)
           #{[1 "Ivan" 19   (- d/tx0 1)]
             [2 "Petr" 22   (- d/tx0 1)]
             [3 "Sergey" 30 (- d/tx0 2)]}))))

(deftest test-resolve-eid-refs
  (let [conn (d/create-conn {:friend {:db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/many
                                      :db/order 0}
                             :name {:db/order 1}})
        tx   (d/transact! conn [{:name "Sergey"
                                 :friend [-1 -2]}
                                [:db/add -1  :name "Ivan"]
                                [:db/add -2  :name "Petr"]
                                [:db/add "B" :name "Boris"]
                                [:db/add "B" :friend -3]
                                [:db/add -3  :name "Oleg"]
                                [:db/add -3  :friend "B"]])
        q '[:find ?fn
            :in $ ?n
            :where [?e :name ?n]
                   [?e :friend ?fe]
                   [?fe :name ?fn]]]
    (is (= (:tempids tx) { -1 2, -2 3, "B" 4, -3 5, :db/current-tx (- d/tx0 1) }))
    (is (= (d/q q @conn "Sergey") #{["Ivan"] ["Petr"]}))
    (is (= (d/q q @conn "Boris") #{["Oleg"]}))
    (is (= (d/q q @conn "Oleg") #{["Boris"]}))))

(deftest test-resolve-current-tx
  (doseq [tx-tempid [:db/current-tx "datomic.tx" "datascript.tx"]]
    (testing tx-tempid
      (let [conn (d/create-conn {:created-at {:db/valueType :db.type/ref
                                              :db/order 0}
                                 :name {:db/order 1}
                                 :prop1 {:db/order 2}
                                 :prop2 {:db/order 3}
                                 :prop3 {:db/order 4}
                                 :prop4 {:db/order 5}})
            tx1  (d/transact! conn [{:name "X", :created-at tx-tempid}
                                    {:db/id tx-tempid, :prop1 "prop1"}
                                    [:db/add tx-tempid :prop2 "prop2"]
                                    [:db/add -1 :name "Y"]
                                    [:db/add -1 :created-at tx-tempid]])]
        (is (= (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)
              #{[1 1 "X"]
                [1 0 (- d/tx0 1)]
                [(- d/tx0 1) 2 "prop1"]
                [(- d/tx0 1) 3 "prop2"]
                [d/tx0 1 "Y"]
                [d/tx0 0 (- d/tx0 1)]}))
        (is (= (:tempids tx1) (assoc {-1 d/tx0, :db/current-tx (- d/tx0 1)}
                                     tx-tempid (- d/tx0 1))))
        (let [tx2   (d/transact! conn [[:db/add tx-tempid :prop3 "prop3"]])
              tx-id (get-in tx2 [:tempids tx-tempid])]
          (is (= tx-id (- d/tx0 2)))
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop3 "prop3"})))
        (let [tx3   (d/transact! conn [{:db/id tx-tempid, :prop4 "prop4"}])
              tx-id (get-in tx3 [:tempids tx-tempid])]
          (is (= tx-id (- d/tx0 3)))
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop4 "prop4"})))))))
