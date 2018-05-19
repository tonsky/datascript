(ns datascript.test.conn
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

(def schema { :aka { :db/cardinality :db.cardinality/many :db/order 0 }
             :name {:db/order 1}
             :age {:db/order 2}
             :sex {:db/order 3}})
(def datoms #{(d/datom 1 0  17)
              (d/datom 1 1 "Ivan")})

(deftest test-ways-to-create-conn
  (let [conn (d/create-conn)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= nil (:schema @conn))))
  
  (let [conn (d/create-conn schema)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= schema (:schema @conn))))
  
  (let [conn (d/conn-from-datoms datoms)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= nil (:schema @conn))))
  
  (let [conn (d/conn-from-datoms datoms schema)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= schema (:schema @conn))))
  
  (let [conn (d/conn-from-db (d/init-db datoms))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= nil (:schema @conn))))
  
  (let [conn (d/conn-from-db (d/init-db datoms schema))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= schema (:schema @conn)))))

(deftest test-reset-conn!
  (let [conn    (d/conn-from-datoms datoms schema)
        report  (atom nil)
        _       (d/listen! conn #(reset! report %))
        datoms' #{(d/datom 1 2 20)
                  (d/datom 1 3 :male)}
        schema' { :email { :db/unique :db.unique/identity }}
        db'     (d/init-db datoms' schema')]
    (d/reset-conn! conn db' :meta)
    (is (= datoms' (set (d/datoms @conn :eavt))))
    (is (= schema' (:schema @conn)))
    
    (let [{:keys [db-before db-after tx-data tx-meta]} @report]
      (is (= datoms  (set (d/datoms db-before :eavt))))
      (is (= schema  (:schema db-before)))
      (is (= datoms' (set (d/datoms db-after :eavt))))
      (is (= schema' (:schema db-after)))
      (is (= :meta   tx-meta))
      (is (= [[1 0  17     false]
              [1 1 "Ivan" false]
              [1 2  20     true]
              [1 3  :male  true]]
             (mapv (juxt :e :a :v :added) tx-data))))))
  
  
