(ns datascript.entity-test
  (:require-macros [cemerick.cljs.test :refer (is deftest testing)])
  (:require [cemerick.cljs.test :as test]
            [datascript.entity
             :refer [entity entity? entity-id]]
            [datascript :as d]))

(defn create-conn []
  (d/create-conn {:interfaces {:db/cardinality :db.cardinality/many
                               :db/valueType :db.type/ref}
                  :server {:db/valueType :db.type/ref}}))

(defn initial-tx [conn]
  (d/transact! conn [[:db/add -10 :name "Server 1"]
                   [:db/add -10 :interfaces -20]
                   [:db/add -10 :interfaces -30]
                   [:db/add -20 :name "eth0"]
                   [:db/add -20 :server -10]
                   [:db/add -30 :name "eth1"]
                   [:db/add -30 :server -10]]))

(deftest test-entity
  (let [conn (create-conn)
        report (initial-tx conn)
        server (entity @conn 1)
        eth0 (entity @conn 2)
        eth1 (entity @conn 3)
        missing (entity @conn 4)]
    (is (entity? server))
    (is (entity? eth1))
    (is (nil? missing))
    (is (not= server eth1))
    (is (not= eth0 eth1))
    (is (= 1 (entity-id server)))
    (is (= 1 1))
    (is (= "#db/entity {:db/id 1}" (pr-str server)))
    (is (= "Server 1" (:name server :nope)))
    (is (= "Server 1" (server :name)))
    (is (= "Server 1" (server :name :nope)))
    (is (= :nope (:wassit server :nope)))
    (is (nil? (:wassit server)))
    (is (= :nada (server :wassit :nada)))
    (is (nil? (server :wassit)))
    (is (= "#db/entity {:db/id 1, :name \"Server 1\"}" (pr-str server)))
    (is (= eth1 (second (:interfaces server))))
    (is (= server (-> server :interfaces first :server)))
    (is (= "eth0" (get-in server [:interfaces 0 :name])))
    ))

(deftest test-entity-lookup
  (let [conn (create-conn)
        report (initial-tx conn)
        server (entity @conn 1)
        eth0 (entity @conn 2)
        eth1 (entity @conn 3)
        missing (entity @conn 3)
        orig-db @conn]

    (testing "From TxReport"
      (is (= server (entity report -10)))
      (is (= server (entity report server)))
      (is (= eth0 (entity report -20)))
      (is (= server (entity report 1)))
      (is (= eth0 (entity report 2)))
      (is (nil? (entity report 999)))
      (is (nil? (entity report -999))))

    (testing "From DB"
      (is (nil? (entity @conn -10)))
      (is (= server (entity @conn server)))
      (is (= server (entity @conn 1)))
      (is (= eth0 (entity @conn 2))))

    (testing "Next DB Version"
      (let [report (d/transact! conn [[:db/add 1 :type :virtual]])]

        (testing "aside: DB equality"
          (is (= orig-db (:db-before report)))
          (is (= @conn (:db-after report)))
          (is (not= orig-db @conn)))

        (is (not= server (entity @conn 1)))

        ))
    ))
