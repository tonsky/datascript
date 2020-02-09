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
  (let [db (d/db-with (d/empty-db {:name  { :db/unique :db.unique/identity }
                                   :email { :db/unique :db.unique/identity }
                                   :slugs { :db/unique      :db.unique/identity
                                            :db/cardinality :db.cardinality/many }})
                      [{:db/id 1 :name "Ivan" :email "@1"}
                       {:db/id 2 :name "Petr" :email "@2"}])
        touched (fn [tx e] (into {} (d/touch (d/entity (:db-after tx) e))))
        tempids (fn [tx] (dissoc (:tempids tx) :db/current-tx))]
    (testing "upsert, no tempid"
      (let [tx (d/with db [{:name "Ivan" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 attrs, no tempid"
      (let [tx (d/with db [{:name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))
    
    (testing "upsert with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {-1 1}))))

    (testing "upsert with string tempid"
      (let [tx (d/with db [{:db/id "1" :name "Ivan" :age 35}
                           [:db/add "2" :name "Oleg"]
                           [:db/add "2" :email "@2"]])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (touched tx 2)
               {:name "Oleg" :email "@2"}))
        (is (= (tempids tx)
               {"1" 1
                "2" 2}))))
    
    (testing "upsert by 2 attrs with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {-1 1}))))
    
    (testing "upsert to two entities, resolve to same tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -1 :name "Ivan" :age 36}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 36}))
        (is (= (tempids tx)
               {-1 1}))))

    (testing "upsert to two entities, two tempids"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -2 :name "Ivan" :age 36}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 36}))
        (is (= (tempids tx)
               {-1 1, -2 1}))))

    (testing "upsert with existing id"
      (let [tx (d/with db [{:db/id 1 :name "Ivan" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 attrs with existing id"
      (let [tx (d/with db [{:db/id 1 :name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert conficts with existing id"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id 2"
        (d/with db [{:db/id 2 :name "Ivan" :age 36}]))))

    (testing "upsert conficts with non-existing id"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id 3"
        (d/with db [{:db/id 3 :name "Ivan" :age 36}]))))
    
    (testing "upsert by non-existing value resolves as update"
      (let [tx (d/with db [{:name "Ivan" :email "@3" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@3" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 conflicting fields"
      (is (thrown-with-msg? Throwable #"Conflicting upserts: \[:name \"Ivan\"\] resolves to 1, but \[:email \"@2\"\] resolves to 2"
        (d/with db [{:name "Ivan" :email "@2" :age 35}]))))

    (testing "upsert over intermediate db"
      (let [tx (d/with db [{:name "Igor" :age 35}
                           {:name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {3 3}))))
    
    (testing "upsert over intermediate db, tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -1 :name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {-1 3}))))

    (testing "upsert over intermediate db, different tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -2 :name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {-1 3, -2 3}))))

    (testing "upsert and :current-tx conflict"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id \d+"
        (d/with db [{:db/id :db/current-tx :name "Ivan" :age 35}]))))

    (testing "upsert of unique, cardinality-many values"
      (let [tx  (d/with db [{:name "Ivan" :slugs "ivan1"}
                            {:name "Petr" :slugs "petr1"}])
            tx2 (d/with (:db-after tx) [{:name "Ivan" :slugs ["ivan1" "ivan2"]}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :slugs #{"ivan1"}}))
        (is (= (touched tx2 1)
               {:name "Ivan" :email "@1" :slugs #{"ivan1" "ivan2"}}))
        (is (thrown-with-msg? Throwable #"Conflicting upserts:"
              (d/with (:db-after tx) [{:slugs ["ivan1" "petr1"]}])))))
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
