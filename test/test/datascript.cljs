(ns test.datascript
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [datascript.core :as dc]
    [datascript :as d]
    [cljs.reader]
    [cemerick.cljs.test :as t]))

(enable-console-print!)

(deftest test-with
  (let [db  (-> (d/empty-db {:aka { :db/cardinality :db.cardinality/many }})
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
               #{["Petr"]}))))))

(deftest test-retract-fns
  (let [db (-> (d/empty-db {:aka    { :db/cardinality :db.cardinality/many }
                            :friend { :db/valueType :db.type/ref }})
               (d/db-with [ { :db/id 1, :name  "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2 }
                            { :db/id 2, :name  "Petr", :age 37 } ]))]
    (let [db (d/db-with db [ [:db.fn/retractEntity 1] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))

    (testing "Retract entitiy with incoming refs"
      (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
             #{[2]}))
      
      (let [db (d/db-with db [ [:db.fn/retractEntity 2] ])]
        (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
               #{}))))
    
    (let [db (d/db-with db [ [:db.fn/retractAttribute 1 :name] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))

    (let [db (d/db-with db [ [:db.fn/retractAttribute 1 :aka] ])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:name "Ivan"] [:age 15] [:friend 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))))

(deftest test-transact!
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many }})]
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
  (let [conn (d/create-conn)]
    (d/transact! conn [[:db/add 1 :weight 200]])
    (d/transact! conn [[:db.fn/cas 1 :weight 200 300]])
    (is (= (:weight (d/entity @conn 1)) 300))
    (try
      (d/transact! conn [[:db.fn/cas 1 :weight 200 210]])
      (throw (js/Error. "expected :db.fn/cas to throw"))
      (catch js/Error e
        (is (= (.-message e) ":db.fn/cas failed on datom [1 :weight 300], expected 200")))))
  
  (let [conn (d/create-conn {:label { :db/cardinality :db.cardinality/many }})]
    (d/transact! conn [[:db/add 1 :label :x]])
    (d/transact! conn [[:db/add 1 :label :y]])
    (d/transact! conn [[:db.fn/cas 1 :label :y :z]])
    (is (= (:label (d/entity @conn 1)) #{:x :y :z}))
    (try
      (d/transact! conn [[:db.fn/cas 1 :label :s :t]])
      (throw (js/Error. "expected :db.fn/cas to throw"))
      (catch js/Error e
        (is (= (.-message e) ":db.fn/cas failed on datom [1 :label (:x :y :z)], expected :s"))))))

(deftest test-db-fn
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many }})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                  db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid :had-birthday true]]
                    (throw (js/Error. (str "No entity with name: " name)))))]
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
    (is (thrown-with-msg? js/Error #"No entity with name: Bob"
                          (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
          e (d/entity db-after 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))


(deftest test-resolve-eid
  (let [conn (d/create-conn)
        t1   (d/transact! conn [[:db/add -1 :name "Ivan"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Petr"]
                                [:db/add -2 :age 22]])
        t2   (d/transact! conn [[:db/add -1 :name "Sergey"]
                                [:db/add -1 :age 30]])]
    (is (= (:tempids t1) { -1 1, -2 2, :db/current-tx (+ d/tx0 1) }))
    (is (= (:tempids t2) { -1 3, :db/current-tx (+ d/tx0 2) }))
    (is (= (d/q '[:find  ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                         [?e :age ?a]] @conn)
           #{[1 "Ivan" 19   (+ d/tx0 1)]
             [2 "Petr" 22   (+ d/tx0 1)]
             [3 "Sergey" 30 (+ d/tx0 2)]}))))

(deftest test-resolve-eid-refs
  (let [conn (d/create-conn {:friend {:db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/many}})
        tx   (d/transact! conn [{:name "Sergey"
                                 :friend [-1 -2]}
                                [:db/add -1 :name "Ivan"]
                                [:db/add -2 :name "Petr"]
                                [:db/add -4 :name "Boris"]
                                [:db/add -4 :friend -3]
                                [:db/add -3 :name "Oleg"]
                                [:db/add -3 :friend -4]])
        q '[:find ?fn
            :in $ ?n
            :where [?e :name ?n]
                   [?e :friend ?fe]
                   [?fe :name ?fn]]]
    (is (= (:tempids tx) { -1 2, -2 3, -4 4, -3 5, :db/current-tx (+ d/tx0 1) }))
    (is (= (d/q q @conn "Sergey") #{["Ivan"] ["Petr"]}))
    (is (= (d/q q @conn "Boris") #{["Oleg"]}))
    (is (= (d/q q @conn "Oleg") #{["Boris"]}))))

(deftest test-resolve-current-tx
  (let [conn (d/create-conn {:created-at {:db/valueType :db.type/ref}})
        tx1  (d/transact! conn [{:name "X"
                                 :created-at :db/current-tx}
                                {:db/id :db/current-tx
                                 :prop1 "prop1"}
                                [:db/add :db/current-tx :prop2 "prop2"]
                                [:db/add -1 :name "Y"]
                                [:db/add -1 :created-at :db/current-tx]])]
    (is (= (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)
           #{[1 :name "X"]
             [1 :created-at (+ d/tx0 1)]
             [(+ d/tx0 1) :prop1 "prop1"]
             [(+ d/tx0 1) :prop2 "prop2"]
             [2 :name "Y"]
             [2 :created-at (+ d/tx0 1)]}))
    (is (= (:tempids tx1) {-1 2, :db/current-tx (+ d/tx0 1)}))
    (let [tx2   (d/transact! conn [[:db/add :db/current-tx :prop3 "prop3"]])
          tx-id (get-in tx2 [:tempids :db/current-tx])]
      (is (= tx-id (+ d/tx0 2)))
      (is (= (into {} (d/entity @conn tx-id))
             {:prop3 "prop3"})))))

(deftest test-entity
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                           {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}]))
        e  (d/entity db 1)]
    (is (= (:db/id e) 1))
    (is (identical? (d/entity-db e) db))
    (is (= (:name e) "Ivan"))
    (is (= (:age  e) 19))
    (is (= (:aka  e) #{"X" "Y"}))
    (is (= (into {} e)
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 1))
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 2))
           {:name "Ivan", :sex "male", :aka #{"Z"}}))

    (is (= (pr-str (d/entity db 1) "{:db/id 1}")))
    (is (= (pr-str (let [e (d/entity db 1)] (:name e) e)) "{:name \"Ivan\", :db/id 1}"))
    (is (= (pr-str (let [e (d/entity db 1)] (:unknown e) e)) "{:db/id 1}"))))

(deftest test-entity-refs
  (let [db (-> (d/empty-db {:father   {:db/valueType   :db.type/ref}
                            :children {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}})
               (d/db-with
                 [{:db/id 1, :children [10]}
                  {:db/id 10, :father 1, :children [100 101]}
                  {:db/id 100, :father 10}]))
        e  #(d/entity db %)]

    (is (= (:children (e 1))   #{(e 10)}))
    (is (= (:children (e 10))  #{(e 100) (e 101)}))

    (testing "empty attribute"
      (is (= (:children (e 100)) nil)))

    (testing "nested navigation"
      (is (= (-> (e 1) :children first :children) #{(e 100) (e 101)}))
      (is (= (-> (e 10) :children first :father) (e 10)))
      (is (= (-> (e 10) :father :children) #{(e 10)}))

      (testing "after touch"
        (let [e1  (e 1)
              e10 (e 10)]
          (d/touch e1)
          (d/touch e10)
          (is (= (-> e1 :children first :children) #{(e 100) (e 101)}))
          (is (= (-> e10 :children first :father) (e 10)))
          (is (= (-> e10 :father :children) #{(e 10)})))))

    (testing "backward navigation"
      (is (= (:_children (e 1))  nil))
      (is (= (:_father   (e 1))  #{(e 10)}))
      (is (= (:_children (e 10)) #{(e 1)}))
      (is (= (:_father   (e 10)) #{(e 100)}))
      (is (= (-> (e 100) :_children first :_children) #{(e 1)}))
    )))

(deftest test-components
  (is (thrown-with-msg? js/Error #"Bad attribute specification for :profile"
        (d/empty-db {:profile {:db/isComponent true}})))
  (is (thrown-with-msg? js/Error #"Bad attribute specification for :profile"
        (d/empty-db {:profile {:db/isComponent "aaa" :db/valueType :db.type/ref}})))
  
  (let [db (d/db-with
             (d/empty-db {:profile {:db/valueType   :db.type/ref
                                    :db/isComponent true}})
             [{:db/id 1 :name "Ivan" :profile 3}
              {:db/id 3 :email "@3"}
              {:db/id 4 :email "@4"}])
        visible #(cljs.reader/read-string (pr-str %))
        touched #(visible (d/touch %))]
    
    (testing "touch"
      (is (= (touched (d/entity db 1))
             {:db/id 1
              :name "Ivan"
              :profile {:db/id 3
                        :email "@3"}}))
      (is (= (touched (d/entity (d/db-with db [[:db/add 3 :profile 4]]) 1))
             {:db/id 1
              :name "Ivan"
              :profile {:db/id 3
                        :email "@3"
                        :profile {:db/id 4
                                  :email "@4"}}})))
    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity 1]])]
        (is (= (d/q '[:find ?a ?v :where [1 ?a ?v]] db)
               #{}))
        (is (= (d/q '[:find ?a ?v :where [3 ?a ?v]] db)
               #{}))))
    
    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute 1 :profile]])]
        (is (= (d/q '[:find ?a ?v :where [3 ?a ?v]] db)
               #{}))))
    
    (testing "reverse navigation"
      (is (= (visible (:_profile (d/entity db 3)))
             {:db/id 1})))))

(deftest test-components-multival
  (let [db (d/db-with
             (d/empty-db {:profile {:db/valueType   :db.type/ref
                                    :db/cardinality :db.cardinality/many
                                    :db/isComponent true}})
             [{:db/id 1 :name "Ivan" :profile [3 4]}
              {:db/id 3 :email "@3"}
              {:db/id 4 :email "@4"}])
        visible #(cljs.reader/read-string (pr-str %))
        touched #(visible (d/touch %))]
    
    (testing "touch"
      (is (= (touched (d/entity db 1))
             {:db/id 1
              :name "Ivan"
              :profile #{{:db/id 3 :email "@3"}
                         {:db/id 4 :email "@4"}}})))
    
    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity 1]])]
        (is (= (d/q '[:find ?a ?v :in $ [?e ....] :where [?e ?a ?v]] db [1 3 4])
               #{}))))
    
    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute 1 :profile]])]
        (is (= (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [3 4])
               #{}))))
    
    (testing "reverse navigation"
      (is (= (visible (:_profile (d/entity db 3)))
             {:db/id 1})))))

(deftest test-listen!
  (let [conn    (d/create-conn)
        reports (atom [])]
    (d/transact! conn [[:db/add -1 :name "Alex"]
                       [:db/add -2 :name "Boris"]])
    (d/listen! conn :test #(swap! reports conj %))
    (d/transact! conn [[:db/add -1 :name "Dima"]
                       [:db/add -1 :age 19]
                       [:db/add -2 :name "Evgeny"]] {:some-metadata 1})
    (d/transact! conn [[:db/add -1 :name "Fedor"]
                       [:db/add 1 :name "Alex2"]         ;; should update
                       [:db/retract 2 :name "Not Boris"] ;; should be skipped
                       [:db/retract 4 :name "Evgeny"]])
    (d/unlisten! conn :test)
    (d/transact! conn [[:db/add -1 :name "Geogry"]])
    
    (is (= (:tx-data (first @reports))
           [(dc/Datom. 3 :name "Dima"   (+ d/tx0 2) true)
            (dc/Datom. 3 :age 19        (+ d/tx0 2) true)
            (dc/Datom. 4 :name "Evgeny" (+ d/tx0 2) true)]))
    (is (= (:tx-meta (first @reports))
           {:some-metadata 1}))
    (is (= (:tx-data (second @reports))
           [(dc/Datom. 5 :name "Fedor"  (+ d/tx0 3) true)
            (dc/Datom. 1 :name "Alex"   (+ d/tx0 3) false)  ;; update -> retract
            (dc/Datom. 1 :name "Alex2"  (+ d/tx0 3) true)   ;;         + add
            (dc/Datom. 4 :name "Evgeny" (+ d/tx0 3) false)]))
    (is (= (:tx-meta (second @reports))
           nil))
    ))

(deftest test-explode
  (doseq [coll [["Devil" "Tupen"]
                #{"Devil" "Tupen"}
                '("Devil" "Tupen")
                #js ["Devil" "Tupen"]]]
    (testing coll
      (let [conn (d/create-conn { :aka { :db/cardinality :db.cardinality/many }
                                 :also { :db/cardinality :db.cardinality/many} })]
        (d/transact! conn [{:db/id -1
                            :name  "Ivan"
                            :age   16
                            :aka   coll
                            :also  "ok"}])
        (is (= (d/q '[:find  ?n ?a
                      :where [1 :name ?n]
                      [1 :age ?a]] @conn)
               #{["Ivan" 16]}))
        (is (= (d/q '[:find  ?v
                      :where [1 :also ?v]] @conn)
               #{["ok"]}))
        (is (= (d/q '[:find  ?v
                      :where [1 :aka ?v]] @conn)
               #{["Devil"] ["Tupen"]}))))))

(deftest test-explode-ref
  (let [db0 (d/empty-db { :children { :db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/many } })]
    (let [db (d/db-with db0 [{:db/id -1, :name "Ivan", :children [-2 -3]}
                             {:db/id -2, :name "Petr"} 
                             {:db/id -3, :name "Evgeny"}])]
      (is (= (d/q '[:find ?n
                    :where [_ :children ?e]
                           [?e :name ?n]] db)
             #{["Petr"] ["Evgeny"]})))
    
    (let [db (d/db-with db0 [{:db/id -1, :name "Ivan"}
                             {:db/id -2, :name "Petr", :_children -1} 
                             {:db/id -3, :name "Evgeny", :_children -1}])]
      (is (= (d/q '[:find ?n
                    :where [_ :children ?e]
                           [?e :name ?n]] db)
             #{["Petr"] ["Evgeny"]})))))

(deftest test-joins
  (let [db (-> (d/empty-db)
               (d/db-with [ { :db/id 1, :name  "Ivan", :age   15 }
                            { :db/id 2, :name  "Petr", :age   37 }
                            { :db/id 3, :name  "Ivan", :age   37 }
                            { :db/id 4, :age 15 }]))]
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                         [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                         [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                         [?e :age ?a]
                         [?e2 :age ?a]
                         [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))))


(deftest test-q-many
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [ [:db/add 1 :name "Ivan"]
                            [:db/add 1 :aka  "ivolga"]
                            [:db/add 1 :aka  "pi"]
                            [:db/add 2 :name "Petr"]
                            [:db/add 2 :aka  "porosenok"]
                            [:db/add 2 :aka  "pi"] ]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where [?e1 :aka ?x]
                         [?e2 :aka ?x]
                         [?e1 :name ?n1]
                         [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))))


(deftest test-q-coll
  (let [db [ [1 :name "Ivan"]
             [1 :age  19]
             [1 :aka  "dragon_killer_94"]
             [1 :aka  "-=autobot=-"] ] ]
    (is (= (d/q '[ :find  ?n ?a
                   :where [?e :aka "dragon_killer_94"]
                          [?e :name ?n]
                          [?e :age  ?a]] db)
           #{["Ivan" 19]})))

  (testing "Query over long tuples"
    (let [db [ [1 :name "Ivan" 945 :db/add]
               [1 :age  39     999 :db/retract]] ]
      (is (= (d/q '[ :find  ?e ?v
                     :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (d/q '[ :find  ?e ?a ?v ?t
                     :where [?e ?a ?v ?t :db/retract]] db)
             #{[1 :age 39 999]})))))


(deftest test-q-in
  (let [db (-> (d/empty-db)
               (d/db-with [ { :db/id 1, :name  "Ivan", :age   15 }
                            { :db/id 2, :name  "Petr", :age   37 }
                            { :db/id 3, :name  "Ivan", :age   37 }]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (d/q query db :age 37)
           #{[2] [3]}))

    (testing "Named DB"
      (is (= (d/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))

    (testing "DB join with collection"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                           [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Relation binding"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Tuple binding"
      (is (= (d/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                           [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))

    (testing "Collection binding"
      (is (= (d/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]})))
    
    (testing "Empty coll handling"
      (is (= (d/q '[:find ?id
                    :in $ [?id ...]
                    :where [?id :age _]]
               [[1 :name "Ivan"]
                [2 :name "Petr"]])
             #{}))
      (is (= (d/q '[:find ?id
                    :in $ [[?id]]
                    :where [?id :age _]]
               [[1 :name "Ivan"]
                [2 :name "Petr"]])
             #{}))))

  (testing "Query without DB"
    (is (= (d/q '[:find ?a ?b
                  :in   ?a ?b]
                10 20)
           #{[10 20]}))))


(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                       [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                       [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))

(deftest test-query-fns
  (testing "ground"
    (is (= (d/q '[:find ?vowel
                  :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
           #{[:a] [:e] [:i] [:o] [:u]})))
  
  (testing "predicate without free variables"
    (is (= (d/q '[:find ?x
                  :in [?x ...]
                  :where [(> 2 1)]] [:a :b :c])
           #{[:a] [:b] [:c]})))

  (let [db (-> (d/empty-db {:parent {:parent {:db/valueType :db.valueType/ref}}})
               (d/db-with [ { :db/id 1, :name  "Ivan",  :age   15 }
                            { :db/id 2, :name  "Petr",  :age   22, :height 240 :parent 1}
                            { :db/id 3, :name  "Slava", :age   37 :parent 2}]))]

    (testing "get-else"
      (is (= (d/q '[:find ?e ?age ?height
                    :in $
                    :where [?e :age ?age]
                           [(get-else $ ?e :height 300) ?height]] db)
             #{[1 15 300] [2 22 240] [3 37 300]})))

    (testing "get-some"
      (is (= (d/q '[:find ?e ?v
                    :in $
                    :where [?e :name ?name]
                           [(get-some $ ?e :height :age) ?v]] db)
             #{[1 15] [2 240] [3 37]})))

    (testing "missing?"
      (is (= (d/q '[:find ?e ?age
                    :in $
                    :where [?e :age ?age]
                           [(missing? $ ?e :height)]] db)
             #{[1 15] [3 37]})))

    (testing "missing? back-ref"
      (is (= (d/q '[:find ?e
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :_parent)]] db)
             #{[3]})))

    (testing "Built-in predicate"
      (is (= (d/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [(< ?a1 18 ?a2)]] db)
             #{[1 2] [1 3]})))

    (testing "Passing predicate as source"
      (is (= (d/q '[:find  ?e
                    :in    $ ?adult
                    :where [?e :age ?a]
                           [(?adult ?a)]]
                  db
                  #(> % 18))
             #{[2] [3]})))

    (testing "Calling a function"
      (is (= (d/q '[:find  ?e1 ?e2 ?e3
                    :where [?e1 :age ?a1]
                           [?e2 :age ?a2]
                           [?e3 :age ?a3]
                           [(+ ?a1 ?a2) ?a12]
                           [(= ?a12 ?a3)]]
                  db)
             #{[1 2 3] [2 1 3]})))
    
    (testing "Function on empty rel"
      (is (= (d/q '[:find  ?e ?y
                    :where [?e :salary ?x]
                           [(+ ?x 100) ?y]]
                  [[0 :age 15] [1 :age 35]])
             #{})))
    ))


(deftest test-rules
  (let [db [                  [5 :follow 3]
            [1 :follow 2] [2 :follow 3] [3 :follow 4] [4 :follow 6]
                          [2         :follow           4]]]
    (is (= (d/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
               '[[(follow ?x ?y)
                  [?x :follow ?y]]])
           #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))
    
    (testing "Joining regular clauses with rule"
      (is (= (d/q '[:find ?y ?x
                    :in $ %
                    :where [_ _ ?x]
                           (rule ?x ?y)
                           [(even? ?x)]]
                  db
                  '[[(rule ?a ?b)
                     [?a :follow ?b]]])
             #{[3 2] [6 4] [4 2]})))
    
    (testing "Rule context is isolated from outer context"
      (is (= (d/q '[:find ?x
                    :in $ %
                    :where [?e _ _]
                           (rule ?x)]
                  db
                  '[[(rule ?e)
                     [_ ?e _]]])
             #{[:follow]})))

    (testing "Rule with branches"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e2 ?e1)
                    [?e2 :follow ?e1]]
                   [(follow ?e2 ?e1)
                    [?e2 :follow ?t]
                    [?t  :follow ?e1]]])
             #{[2] [3] [4]})))

    (testing "Recursive rules"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                 '[[(follow ?e1 ?e2)
                    [?e1 :follow ?e2]]
                   [(follow ?e1 ?e2)
                    [?e1 :follow ?t]
                    (follow ?t ?e2)]])
             #{[2] [3] [4] [6]}))

      (is (= (d/q '[:find ?e1 ?e2
                     :in $ %
                     :where (follow ?e1 ?e2)]
                    [[1 :follow 2] [2 :follow 3]]
                   '[[(follow ?e1 ?e2)
                      [?e1 :follow ?e2]]
                     [(follow ?e1 ?e2)
                      (follow ?e2 ?e1)]])
           #{[1 2] [2 3] [2 1] [3 2]}))

      (is (= (d/q '[:find ?e1 ?e2
                     :in $ %
                     :where (follow ?e1 ?e2)]
                    [[1 :follow 2] [2 :follow 3] [3 :follow 1]]
                   '[[(follow ?e1 ?e2)
                      [?e1 :follow ?e2]]
                     [(follow ?e1 ?e2)
                      (follow ?e2 ?e1)]])
           #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]})))

    (testing "Mutually recursive rules"
      (is (= (d/q '[:find  ?e1 ?e2
                    :in    $ %
                    :where (f1 ?e1 ?e2)]
                  [[0 :f1 1]
                   [1 :f2 2]
                   [2 :f1 3]
                   [3 :f2 4]
                   [4 :f1 5]
                   [5 :f2 6]]
                 '[[(f1 ?e1 ?e2)
                    [?e1 :f1 ?e2]]
                   [(f1 ?e1 ?e2)
                    [?t :f1 ?e2]
                    (f2 ?e1 ?t)]
                   [(f2 ?e1 ?e2)
                    [?e1 :f2 ?e2]]
                   [(f2 ?e1 ?e2)
                    [?t :f2 ?e2]
                    (f1 ?e1 ?t)]])
            #{[0 1] [0 3] [0 5]
              [1 3] [1 5]
              [2 3] [2 5]
              [3 5]
              [4 5]})))

    (testing "Passing ins to rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ % ?even
                    :where
                    (match ?even ?x ?y)]
                  db
                  '[[(match ?pred ?e ?e2)
                     [?e :follow ?e2]
                     [(?pred ?e)]
                     [(?pred ?e2)]]]
                  even?)
             #{[4 6] [2 4]})))
    
    (testing "Using built-ins inside rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ %
                    :where (match ?x ?y)]
                  db
                  '[[(match ?e ?e2)
                     [?e :follow ?e2]
                     [(even? ?e)]
                     [(even? ?e2)]]])
             #{[4 6] [2 4]})))
    (testing "Calling rule twice (#44)"
      (d/q '[:find ?p
             :in $ % ?fn
             :where (rule ?p ?fn "a")
                    (rule ?p ?fn "b")]
           [[1 :attr "a"]]
          '[[(rule ?p ?fn ?x)
             [?p :attr ?x]
             [(?fn ?x)]]]
           (constantly true)))
  )


  (testing "Specifying db to rule"
    (is (= (d/q '[ :find ?n
                    :in   $sexes $ages %
                    :where ($sexes male ?n)
                           ($ages adult ?n) ]
                  [["Ivan" :male] ["Darya" :female] ["Oleg" :male] ["Igor" :male]]
                  [["Ivan" 15] ["Oleg" 66] ["Darya" 32]]
                  '[[(male ?x)
                     [?x :male]]
                    [(adult ?y)
                     [?y ?a]
                     [(>= ?a 18)]]])
           #{["Oleg"]})))
  )

(deftest test-aggregates
  (let [monsters [ ["Cerberus" 3]
                   ["Medusa" 1]
                   ["Cyclops" 1]
                   ["Chimera" 1] ]]
    (testing "with"
      (is (= (d/q '[ :find ?heads
                     :with ?monster
                     :in   [[?monster ?heads]] ]
                  [ ["Medusa" 1]
                    ["Cyclops" 1]
                    ["Chimera" 1] ])
             [[1] [1] [1]])))

    (testing "Wrong grouping without :with"
      (is (= (d/q '[ :find (sum ?heads)
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[4]])))

    (testing "Multiple aggregates, correct grouping with :with"
      (is (= (d/q '[ :find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
                     :with ?monster
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[6 1 3 4 2]])))
    
    (testing "Min and max are using comparator instead of default compare"
      ;; Wrong: using js '<' operator
      ;; (apply min [:a/b :a-/b :a/c]) => :a-/b
      ;; (apply max [:a/b :a-/b :a/c]) => :a/c
      ;; Correct: use IComparable interface
      ;; (sort compare [:a/b :a-/b :a/c]) => (:a/b :a/c :a-/b)
      (is (= (d/q '[:find (min ?x) (max ?x)
                    :in [?x ...]]
                  [:a-/b :a/b])
             [[:a/b :a-/b]]))

      (is (= (d/q '[:find (min 2 ?x) (max 2 ?x)
                    :in [?x ...]]
                  [:a/b :a-/b :a/c])
             [[[:a/b :a/c] [:a/c :a-/b]]])))

    (testing "Min and max comparator order types reliably"
      (let [date (js/Date.)]
        ;; Wrong: js '<' operator coerce everything to string
        ;; (apply min ["1" date 1]) => 1
        ;; (apply max ["1" date 1]) => 1
        ;; Correct: dc/cmp-val compares types first
        ;; (sort dc/cmp-val ["1" date 1]) => (date 1 "1")
        (is (= (d/q '[:find (min ?x) (max ?x)
                      :in [?x ...]]
                    ["1" date 1])
               [[date "1"]]))

        (is (= (d/q '[:find (min 2 ?x) (max 2 ?x)
                      :in [?x ...]]
                    ["1" date 1])
               [[[date 1] [1 "1"]]]))))

    (testing "Grouping and parameter passing"
      (is (= (set (d/q '[ :find ?color (max ?amount ?x) (min ?amount ?x)
                          :in   [[?color ?x]] ?amount ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       3))
             #{[:red  [3 4 5] [1 2 3]]
               [:blue [7 8]   [7 8]]})))

    (testing "avg aggregate" 
      (is (= (ffirst (d/q '[:find (avg ?x) 
                            :in [?x ...]]
                           [10 15 20 35 75]))
             31)))

    (testing "median aggregate"
      (is (= (ffirst (d/q '[:find (median ?x)
                            :in [?x ...]]
                           [10 15 20 35 75]))
             20)))
    
    (testing "variance aggregate"
      (is (= (ffirst (d/q '[:find (variance ?x)
                            :in [?x ...]]
                           [10 15 20 35 75]))
              554)))

    (testing "stddev aggregate"
      (is (= (ffirst (d/q '[:find (stddev ?x) 
                            :in [?x ...]]
                          [10 15 20 35 75]))
              23.53720459187964)))

    (testing "Custom aggregates"
      (is (= (set (d/q '[ :find ?color (aggregate ?agg ?x)
                          :in   [[?color ?x]] ?agg ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       #(reverse (sort %))))
             #{[:red [5 4 3 2 1]] [:blue [8 7]]})))))

(deftest test-find-specs
  (let [db (-> (d/empty-db)
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11] ]))]
    (is (= (set (d/q '[:find [?name ...]
                        :where [_ :name ?name]] db))
           #{"Ivan" "Petr" "Sergey"}))
    (is (= (d/q '[:find [?name ?age]
                  :where [1 :name ?name]
                         [1 :age  ?age]] db)
           ["Petr" 44]))
    (is (= (d/q '[:find ?name .
                  :where [1 :name ?name]] db)
           "Petr"))
    
    (testing "Multiple results get cut"
      (is (contains?
            #{["Petr" 44] ["Ivan" 25] ["Sergey" 11]}
            (d/q '[:find [?name ?age]
                   :where [?e :name ?name]
                          [?e :age  ?age]] db)))
      (is (contains?
            #{"Ivan" "Petr" "Sergey"}
            (d/q '[:find ?name .
                   :where [_ :name ?name]] db))))
    
    (testing "Aggregates work with find specs"
      (is (= (d/q '[:find [(count ?name) ...]
                    :where [_ :name ?name]] db)
             [3]))
      (is (= (d/q '[:find [(count ?name)]
                    :where [_ :name ?name]] db)
             [3]))
      (is (= (d/q '[:find (count ?name) .
                    :where [_ :name ?name]] db)
             3)))
    ))

(deftest test-datoms
  (let [dvec #(vector (.-e %) (.-a %) (.-v %))
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
  (let [dvec #(vector (.-e %) (.-a %) (.-v %))
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
  (let [dvec #(vector (.-e %) (.-a %) (.-v %))
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

(deftest test-pr-read
  (binding [cljs.reader/*tag-table* (atom {"datascript/Datom" d/datom-from-reader})]
    (let [d (dc/Datom. 1 :name 3 17 true)]
      (is (= d (cljs.reader/read-string (pr-str d)))))
    (let [d (dc/Datom. 1 :name 3 nil nil)]
      (is (= d (cljs.reader/read-string (pr-str d))))))

  (let [db (-> (d/empty-db)
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11]]))]
    (binding [cljs.reader/*tag-table* (atom {"datascript/DB" d/db-from-reader})]
      (is (= db (cljs.reader/read-string (pr-str db)))))))

(deftest test-filter-db
  (let [db (-> (d/empty-db {:aka { :db/cardinality :db.cardinality/many }})
               (d/db-with [{:db/id 1
                            :name  "Petr"
                            :email "petya@spb.ru"
                            :aka   ["I" "Great"]
                            :password "<SECRET>"}
                           {:db/id 2
                            :name  "Ivan"
                            :aka   ["Terrible" "IV"]
                            :password "<PROTECTED>"}
                           {:db/id 3
                            :name  "Nikolai"
                            :aka   ["II"]
                            :password "<UNKWOWN>"}
                           ]))
        remove-pass (fn [_ datom] (not= :password (.-a datom)))
        remove-ivan (fn [_ datom] (not= 2 (.-e datom)))
        long-akas   (fn [udb datom] (or (not= :aka (.-a datom))
                                        ;; has just 1 aka
                                        (<= (count (:aka (d/entity udb (.-e datom)))) 1)
                                        ;; or aka longer that 4 chars
                                        (>= (count (.-v datom)) 4)))]
    
    (are [_db _res] (= (d/q '[:find ?v :where [_ :password ?v]] _db) _res)
      db                        #{["<SECRET>"] ["<PROTECTED>"] ["<UNKWOWN>"]}
      (d/filter db remove-pass) #{}
      (d/filter db remove-ivan) #{["<SECRET>"] ["<UNKWOWN>"]}
      (-> db (d/filter remove-ivan) (d/filter remove-pass)) #{})

    (are [_db _res] (= (d/q '[:find ?v :where [_ :aka ?v]] _db) _res)
      db                        #{["I"] ["Great"] ["Terrible"] ["IV"] ["II"]}
      (d/filter db remove-pass) #{["I"] ["Great"] ["Terrible"] ["IV"] ["II"]}
      (d/filter db remove-ivan) #{["I"] ["Great"] ["II"]}
      (d/filter db long-akas)   #{["Great"] ["Terrible"] ["II"]}
      (-> db (d/filter remove-ivan) (d/filter long-akas)) #{["Great"] ["II"]}
      (-> db (d/filter long-akas) (d/filter remove-ivan)) #{["Great"] ["II"]})
     
    (testing "Entities"
      (is (= (:password (d/entity db 1)) "<SECRET>"))
      (is (= (:password (d/entity (d/filter db remove-pass) 1) ::not-found) ::not-found))
      (is (= (:aka (d/entity db 2)) #{"Terrible" "IV"}))
      (is (= (:aka (d/entity (d/filter db long-akas) 2)) #{"Terrible"})))
    
    (testing "Index access"
      (is (= (map :v (d/datoms db :aevt :password))
             ["<SECRET>" "<PROTECTED>" "<UNKWOWN>"]))
      (is (= (map :v (d/datoms (d/filter db remove-pass) :aevt :password))
             [])))
  
    (testing "equiv and hash"
      (is (= (d/db-with db [[:db.fn/retractEntity 2]])
             (d/filter db remove-ivan)))
      (is (= (hash (d/db-with db [[:db.fn/retractEntity 2]]))
             (hash (d/filter db remove-ivan)))))))

;; (t/test-ns 'test.datascript)


