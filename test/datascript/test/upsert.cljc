(ns datascript.test.upsert
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-upsert
  (let [ivan    {:db/id 1 :name "Ivan" :email "@1"}
        petr    {:db/id 2 :name "Petr" :email "@2" :ref 3}
        dima    {:db/id 3 :name "Dima" :email "@3" :ref 4}
        olga    {:db/id 4 :name "Olga" :email "@4" :ref 1}
        db      (d/db-with (d/empty-db {:name  {:db/unique      :db.unique/identity}
                                        :email {:db/unique      :db.unique/identity}
                                        :slugs {:db/unique      :db.unique/identity
                                                :db/cardinality :db.cardinality/many}
                                        :ref   {:db/unique      :db.unique/identity
                                                :db/type        :db.type/ref}})
                  [ivan petr dima olga])
        pull    (fn [tx e]
                  (d/pull (:db-after tx) ['* {[:ref :xform #(:db/id %)] [:db/id]}] e))
        tempids (fn [tx]
                  (dissoc (:tempids tx) :db/current-tx))]
    (testing "upsert, no tempid"
      (let [tx (d/with db [{:name "Ivan" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {}))))

    (testing "upsert by 2 attrs, no tempid"
      (let [tx (d/with db [{:name "Ivan" :email "@1" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {}))))
    
    (testing "upsert with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {-1 1}))))

    (testing "upsert with string tempid"
      (let [tx (d/with db [{:db/id "1" :name "Ivan" :age 35}
                           [:db/add "2" :name "Oleg"]
                           [:db/add "2" :email "@2"]])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= {:db/id 2 :name "Oleg" :email "@2" :ref 3}
              (pull tx 2)))
        (is (= (tempids tx)
              {"1" 1
               "2" 2}))))
    
    (testing "upsert by 2 attrs with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :email "@1" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {-1 1}))))
    
    (testing "upsert to two entities, resolve to same tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -1 :name "Ivan" :age 36}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 36}
              (pull tx 1)))
        (is (= (tempids tx)
              {-1 1}))))

    (testing "upsert to two entities, two tempids"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -2 :name "Ivan" :age 36}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 36}
              (pull tx 1)))
        (is (= (tempids tx)
              {-1 1, -2 1}))))

    (testing "upsert with existing id"
      (let [tx (d/with db [{:db/id 1 :name "Ivan" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {}))))

    (testing "upsert by 2 attrs with existing id"
      (let [tx (d/with db [{:db/id 1 :name "Ivan" :email "@1" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {}))))

    (testing "upsert by 2 attrs with existing id as lookup ref"
      (let [tx (d/with db [{:db/id [:name "Ivan"] :name "Ivan" :email "@1" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {}))))

    (testing "upsert conflicts with existing id"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id 2"
            (d/with db [{:db/id 2 :name "Ivan" :age 36}]))))

    (testing "upsert conflicts with non-existing id"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id 5"
            (d/with db [{:db/id 5 :name "Ivan" :age 36}]))))
    
    (testing "upsert by non-existing value resolves as update"
      (let [tx (d/with db [{:name "Ivan" :email "@5" :age 35}])]
        (is (= {:db/id 1 :name "Ivan" :email "@5" :age 35}
              (pull tx 1)))
        (is (= (tempids tx)
              {}))))

    (testing "upsert by 2 conflicting fields"
      (is (thrown-with-msg? Throwable #"Conflicting upserts: \[:name \"Ivan\"\] resolves to 1, but \[:email \"@2\"\] resolves to 2"
            (d/with db [{:name "Ivan" :email "@2" :age 35}]))))

    (testing "upsert over intermediate db"
      (let [tx (d/with db [{:name "Igor" :age 35}
                           {:name "Igor" :age 36}])]
        (is (= {:db/id 5 :name "Igor" :age 36}
              (pull tx 5)))
        (is (= (tempids tx)
              {5 5}))))
    
    (testing "upsert over intermediate db, tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -1 :name "Igor" :age 36}])]
        (is (= {:db/id 5 :name "Igor" :age 36}
              (pull tx 5)))
        (is (= (tempids tx)
              {-1 5}))))

    (testing "upsert over intermediate db, different tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -2 :name "Igor" :age 36}])]
        (is (= {:db/id 5 :name "Igor" :age 36}
              (pull tx 5)))
        (is (= (tempids tx)
              {-1 5, -2 5}))))

    (testing "upsert and :current-tx conflict"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id \d+"
            (d/with db [{:db/id :db/current-tx :name "Ivan" :age 35}]))))

    (testing "upsert of unique, cardinality-many values"
      (let [tx  (d/with db [{:name "Ivan" :slugs "ivan1"}
                            {:name "Petr" :slugs "petr1"}])
            tx2 (d/with (:db-after tx) [{:name "Ivan" :slugs ["ivan1" "ivan2"]}])]
        (is (= {:db/id 1 :name "Ivan" :email "@1" :slugs ["ivan1"]}
              (pull tx 1)))
        (is (= {:db/id 1 :name "Ivan" :email "@1" :slugs ["ivan1" "ivan2"]}
              (pull tx2 1)))
        (is (thrown-with-msg? Throwable #"Conflicting upserts:"
              (d/with (:db-after tx) [{:slugs ["ivan1" "petr1"]}])))))
    
    (testing "upsert by ref"
      (let [tx (d/with db [{:ref 3 :age 36}])]
        (is (= {:db/id 2 :name "Petr" :email "@2" :ref 3 :age 36}
              (pull tx 2))))
      (let [tx (d/with db [{:ref 4 :age 37}])]
        (is (= {:db/id 3 :name "Dima" :email "@3" :ref 4 :age 37}
              (pull tx 3))))
      (let [tx (d/with db [{:ref 1 :age 38}])]
        (is (= {:db/id 4 :name "Olga" :email "@4" :ref 1 :age 38}
              (pull tx 4)))))
    
    (testing "upsert by lookup ref"
      (let [tx (d/with db [{:ref [:name "Dima"] :age 36}])]
        (is (= {:db/id 2 :name "Petr" :email "@2" :ref 3 :age 36}
              (pull tx 2))))
      (let [tx (d/with db [{:ref [:name "Olga"] :age 37}])]
        (is (= {:db/id 3 :name "Dima" :email "@3" :ref 4 :age 37}
              (pull tx 3))))
      (let [tx (d/with db [{:ref [:name "Ivan"] :age 38}])]
        (is (= {:db/id 4 :name "Olga" :email "@4" :ref 1 :age 38}
              (pull tx 4)))))
    
    ;; https://github.com/tonsky/datascript/issues/464
    (testing "not upsert by ref"
      (let [tx (d/with db [{:db/id -1 :name "Igor"}
                           {:db/id -2 :name "Anna" :ref -1}])]
        (is (= {:db/id 5 :name "Igor"} (pull tx 5)))
        (is (= {:db/id 6 :name "Anna" :ref 5} (pull tx 6))))
      
      (let [tx (d/with db [{:db/id "A" :name "Igor"}
                           {:db/id "B" :name "Anna" :ref "A"}])]
        (is (= {:db/id 5 :name "Igor"} (pull tx 5)))
        (is (= {:db/id 6 :name "Anna" :ref 5} (pull tx 6)))))
    
    ))

(deftest test-redefining-ids
  (let [db (-> (d/empty-db {:name { :db/unique :db.unique/identity }})
             (d/db-with [{:db/id -1 :name "Ivan"}]))]
    (let [tx (d/with db [{:db/id -1 :age 35}
                         {:db/id -1 :name "Ivan" :age 36}])]
      (is (= #{[1 :age 36] [1 :name "Ivan"]}
            (tdc/all-datoms (:db-after tx))))
      (is (= {-1 1, :db/current-tx (+ d/tx0 2)}
            (:tempids tx)))))
  
  (let [db (-> (d/empty-db {:name  { :db/unique :db.unique/identity }})
             (d/db-with [{:db/id -1 :name "Ivan"}
                         {:db/id -2 :name "Oleg"}]))]
    (is (thrown-with-msg? Throwable #"Conflicting upsert: -1 resolves both to 1 and 2"
          (d/with db [{:db/id -1 :name "Ivan" :age 35}
                      {:db/id -1 :name "Oleg" :age 36}])))))

;; https://github.com/tonsky/datascript/issues/285
(deftest test-retries-order
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
             (d/db-with [[:db/add -1 :age 42]
                         [:db/add -2 :likes "Pizza"]
                         [:db/add -1 :name "Bob"]
                         [:db/add -2 :name "Bob"]]))]
    (is (= {:db/id 1, :name "Bob", :likes "Pizza", :age 42}
          (tdc/entity-map db 1))))

  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
             (d/db-with [[:db/add -1 :age 42]
                         [:db/add -2 :likes "Pizza"]
                         [:db/add -2 :name "Bob"]
                         [:db/add -1 :name "Bob"]]))]
    (is (= {:db/id 2, :name "Bob", :likes "Pizza", :age 42}
          (tdc/entity-map db 2)))))

;; https://github.com/tonsky/datascript/issues/403
(deftest test-upsert-string-tempid-ref
  (let [db   (-> (d/empty-db {:name {:db/unique :db.unique/identity}
                              :ref {:db/valueType :db.type/ref}})
               (d/db-with [{:name "Alice"}]))
        expected #{[1 :name "Alice"]
                   [2 :age 36]
                   [2 :ref 1]}]
    (is (= expected (tdc/all-datoms
                      (d/db-with db [{:db/id "user", :name "Alice"}
                                     {:age 36, :ref "user"}]))))
    (is (= expected (tdc/all-datoms
                      (d/db-with db [[:db/add "user" :name "Alice"]
                                     {:age 36, :ref "user"}]))))
    (is (= expected (tdc/all-datoms
                      (d/db-with db [{:db/id -1, :name "Alice"}
                                     {:age 36, :ref -1}]))))
    (is (= expected (tdc/all-datoms
                      (d/db-with db [[:db/add -1, :name "Alice"]
                                     {:age 36, :ref -1}]))))))

;; https://github.com/tonsky/datascript/issues/472
(deftest test-two-tempids-two-retries
  (let [schema   {:name {:db/unique :db.unique/identity}
                  :ref {:db/valueType :db.type/ref}}
        db       (d/db-with
                   (d/empty-db schema)
                   [{:name "Alice"}
                    {:name "Bob"}])
        expected #{[1 :name "Alice"]
                   [2 :name "Bob"]
                   [3 :ref 1]
                   [4 :ref 2]}]
    (is (= expected
          (tdc/all-datoms
            (d/db-with db
              [{:db/id 3, :ref "A"}
               {:db/id 4, :ref "B"}
               {:db/id "A", :name "Alice"}
               {:db/id "B", :name "Bob"}]))))))

(deftest test-vector-upsert
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
             (d/db-with [{:db/id -1, :name "Ivan"}]))]
    (are [tx res] (= res (tdc/all-datoms (d/db-with db tx)))
      [[:db/add -1 :name "Ivan"]
       [:db/add -1 :age 12]]
      #{[1 :age 12] [1 :name "Ivan"]}
         
      [[:db/add -1 :age 12]
       [:db/add -1 :name "Ivan"]]
      #{[1 :age 12] [1 :name "Ivan"]}))
  
  (let [db (-> (d/empty-db {:name  { :db/unique :db.unique/identity }})
             (d/db-with [[:db/add -1 :name "Ivan"]
                         [:db/add -2 :name "Oleg"]]))]
    (is (thrown-with-msg? Throwable #"Conflicting upsert: -1 resolves both to 1 and 2"
          (d/with db [[:db/add -1 :name "Ivan"]
                      [:db/add -1 :age 35]
                      [:db/add -1 :name "Oleg"]
                      [:db/add -1 :age 36]])))))
