(ns datascript.test.upsert
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-upsert
  (let [db (d/db-with (d/empty-db {:name  { :db/unique :db.unique/identity }
                                   :email { :db/unique :db.unique/identity }})
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

    (testing "tempids already resolved to new id"
      (is (thrown-with-msg? Throwable #"Cannot resolve upsert"
        (d/with db [{:db/id -1 :age 35}
                    {:db/id -1 :name "Ivan" :age 36}]))))

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
      (is (thrown-with-msg? Throwable #"Cannot resolve upsert"
        (d/with db [{:db/id 2 :name "Ivan" :age 36}]))))

    (testing "upsert conficts with non-existing id"
      (is (thrown-with-msg? Throwable #"Cannot resolve upsert"
        (d/with db [{:db/id 3 :name "Ivan" :age 36}]))))
    
    (testing "upsert by non-existing value resolves as update"
      (let [tx (d/with db [{:name "Ivan" :email "@3" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@3" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 conflicting fields"
      (is (thrown-with-msg? Throwable #"Cannot resolve upsert"
        (d/with db [{:name "Ivan" :email "@2" :age 35}]))))

    (testing "upsert over intermediate db"
      (let [tx (d/with db [{:name "Igor" :age 35}
                           {:name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {}))))
    
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
      (is (thrown-with-msg? Throwable #"Cannot resolve upsert"
        (d/with db [{:db/id :db/current-tx :name "Ivan" :age 35}]))))
))
