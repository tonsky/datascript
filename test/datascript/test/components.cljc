(ns datascript.test.components
  (:require
    [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-components
  (is (thrown-with-msg? Throwable #"Bad attribute specification for :profile"
        (d/empty-db {:profile {:db/isComponent true}})))
  (is (thrown-with-msg? Throwable #"Bad attribute specification for [{]:profile [{]:db/isComponent \"aaa\"}}"
        (d/empty-db {:profile {:db/isComponent "aaa" :db/valueType :db.type/ref}})))
  
  (let [db (d/db-with
             (d/empty-db {:profile {:db/valueType   :db.type/ref
                                    :db/isComponent true}})
             [{:db/id 1 :name "Ivan" :profile 3}
              {:db/id 3 :email "@3"}
              {:db/id 4 :email "@4"}])
        visible #(edn/read-string (pr-str %))
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
        visible #(edn/read-string (pr-str %))
        touched #(visible (d/touch %))]
    
    (testing "touch"
      (is (= (touched (d/entity db 1))
             {:db/id 1
              :name "Ivan"
              :profile #{{:db/id 3 :email "@3"}
                         {:db/id 4 :email "@4"}}})))
    
    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity 1]])]
        (is (= (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [1 3 4])
               #{}))))
    
    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute 1 :profile]])]
        (is (= (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [3 4])
               #{}))))
    
    (testing "reverse navigation"
      (is (= (visible (:_profile (d/entity db 3)))
             {:db/id 1})))))
