(ns datascript.extensions-test
  (:require-macros [cemerick.cljs.test :refer (is deftest)])
  (:require [cemerick.cljs.test :as test]
            [datascript.extensions :as exts
             :refer [entity entity? entity-id transact!
                     delete-entity new-entity]]
            [datascript :as d]))

(deftest explode
  (let [conn (exts/create-conn {} nil)
        eid 123
        k :eggs]
    (is (= [[:db.fn/retractAttribute eid k]]
           (exts/explode @conn {:db/id eid k nil} nil)))
    (is (= []
           (exts/explode @conn {:db/id eid k nil} {:first-load? true})))))

(def conn-schema-keys (comp set keys :schema deref))

(defn create-conn [one-rels many-rels]
  (exts/reset-tempids)
  (doto (exts/create-conn {} nil)
    (swap! exts/with-many-rel many-rels)
    (swap! exts/with-one-rel one-rels)))

(deftest change-schema
  (is (= #{:a :b :x}
         (conn-schema-keys (create-conn [:a :b] [:x]))))
  (is (= #{:a :b :x}
         (conn-schema-keys
           (doto (exts/create-conn {} nil)
             (swap! exts/with-many-rel :a :b)
             (swap! exts/with-one-rel :x)))))
  (is (= #{:a :b :x}
         (conn-schema-keys
           (doto (exts/create-conn {} nil)
             (swap! exts/with-many-rel [:a :b])
             (swap! exts/with-one-rel [:x]))))))

(defn base-db []
  (let [conn (create-conn [:server] [:interfaces])]
    (transact! conn [{:id 1 :name "Server 1"
                      :meta {:this "will be lost"}
                      :interfaces
                      [{:id 2 :name "eth0" :server {:id 1 :meta {}}}
                       {:id 3 :name "eth1" :server {:id 1 :meta {}}}]}])
    conn))

(def base-history
  [[1 :name "Server 1" 536870913 true]
   [1 :meta {:this "will be lost"} 536870913 true]
   [1 :interfaces 2 536870913 true]
   [2 :name "eth0" 536870913 true]
   [2 :server 1 536870913 true]
   [1 :meta {:this "will be lost"} 536870913 false]
   [1 :meta {} 536870913 true]
   [1 :interfaces 3 536870913 true]
   [3 :name "eth1" 536870913 true]
   [3 :server 1 536870913 true]])

(deftest test-entity
  (let [conn (base-db)
        server (entity @conn 1)
        eth0 (entity @conn 2)
        eth1 (entity @conn 3)]
    (is (entity? server))
    (is (entity? eth1))
    (is (not= server eth1))
    (is (not= eth0 eth1))
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


(defn ->commands [db e]
  (exts/explode-all db (exts/get-commands e) nil))

(defn history-info [conn]
  (map (fn [d] [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)])
       (exts/history @conn)))

(deftest change-entity
  (let [conn (base-db)
        server (entity @conn 1)]
    (is (= [] (->commands @conn server)))
    (is (= 35 (:age (assoc server :age 35))))
    (is (nil? (:age server)))
    (is (= [[:db/add 1 :age 35]]
           (->commands @conn (assoc server :age 35))))
    (is (= [[:db.fn/retractAttribute 1 :name]]
           (->commands @conn (dissoc server :name))))
    (is (= "outgoing"
           (get-in (assoc-in server [:interfaces 1 :direction] "outgoing")
                   [:interfaces 1 :direction])))
    (is (= [[:db.fn/retractAttribute 1 :interfaces]
            [:db/add 1 :interfaces 2]
            [:db/add 1 :interfaces 3]
            [:db/add 3 :direction "outgoing"]]
           (->commands @conn (assoc-in server [:interfaces 1 :direction] "outgoing"))))
    (is (= [[:db.fn/retractAttribute 1 :interfaces]
            [:db/add 1 :interfaces 2]
            [:db/add 1 :interfaces 3]
            [:db.fn/retractAttribute 3 :server]]
           (->commands @conn (update-in server [:interfaces 1] dissoc :server)))))

  (let [conn (base-db)
        server (entity @conn 1)]
    (exts/transact! conn [(assoc-in server [:interfaces 1 :direction] "outgoing")])
    ; Note that interfaces 2 and 3 are removed and then subsequently added.
    (is (= (concat
             base-history
             [[1 :interfaces 2 536870914 false]
              [1 :interfaces 3 536870914 false]
              [1 :interfaces 2 536870914 true]
              [1 :interfaces 3 536870914 true]
              [3 :direction "outgoing" 536870914 true]])
           (history-info conn))))
  )

(deftest test-new-entity
  (let [conn (base-db)
        server2 (new-entity @conn {:name "Server 2"})]
    (is (entity? (new-entity @conn)))
    (is (= [] (->commands @conn (new-entity @conn))))
    (is (= [[:db/add -1 :name "Server 2"]] (->commands @conn server2)))
    (is (neg? (entity-id server2)))
    (is (exts/tempid? server2))
    (let [r (exts/transact! conn [server2])
          server2' (entity r server2)]
      (is (= (:ident (:db-after r)) (:ident @conn)))
      (is server2')
      (is (not= server2 server2'))
      (is (exts/created? server2'))
      (is (= "Server 2" (:name server2')))
      (is (pos? (entity-id server2')))
      (is (entity? (new-entity @conn {:name "eth0" :server server2'}))
          "server2' is on the same tx as the connection")
      )))


(deftest test-new-entity-bad
  (let [conn (base-db)
        server2 (new-entity @conn {:name "Server 2"})
        r (exts/transact! conn [server2])]
    ; associate wrong version of server2:
    (is (thrown-with-msg?
          js/Error #"previous transaction"
          (new-entity @conn {:name "eth0" :server server2})))))

(deftest test-delete-entity
  (let [conn (base-db)
        server2 (new-entity @conn {:name "Server 2"})
        r (exts/transact! conn [server2])
        rm-server (delete-entity @conn 1)
        rm-server2 (delete-entity @conn (entity r server2))
        r (exts/transact! conn [rm-server rm-server2])]
    (is (entity? rm-server))
    (is (exts/delete? rm-server))
    (is (exts/delete? rm-server2))
    (is (= {} (into {} (entity r rm-server))))
    (is (= {} (into {} (entity r rm-server2))))
    (is (= (concat
             base-history
             [[1342177281 :name "Server 2" 536870914 true]
              [1 :interfaces 2 536870915 false]
              [1 :interfaces 3 536870915 false]
              [1 :meta {} 536870915 false]
              [1 :name "Server 1" 536870915 false]
              [1342177281 :name "Server 2" 536870915 false]])
           (history-info conn)))))
