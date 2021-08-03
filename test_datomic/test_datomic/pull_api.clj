(ns test-datomic.pull-api
  "Mirrors datascript.test.pull-api with Datomic to check they behave the same"
  (:require
   [clojure.test :refer :all]
   [datomic.api :as datomic]))

(def ^:private test-schema
  {:person/name   {:db/unique :db.unique/identity}
   :person/aka    {:db/cardinality :db.cardinality/many}
   :person/child  {:db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/ref}
   :person/friend {:db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/ref}
   :person/enemy  {:db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/ref}
   :person/father {:db/valueType :db.type/ref}

   :thing/name    {:db/unique :db.unique/identity}
   :thing/part    {:db/valueType   :db.type/ref
                   :db/isComponent true
                   :db/cardinality :db.cardinality/many}
   :thing/spec    {:db/valueType   :db.type/ref
                   :db/isComponent true
                   :db/cardinality :db.cardinality/one}})

(defn- datomic-schema-attr [[name args]]
  (merge
    {:db/ident              name
     :db/valueType          :db.type/string
     :db/cardinality        :db.cardinality/one
     :db.install/_attribute :db.part/db}
    args))

(def ^:private test-datomic-schema
  (mapv datomic-schema-attr test-schema))

(def tmpids (atom nil))

(def test-data
  [{:db/id        "petrid"
    :person/name  "Petr"
    :person/aka   ["Devil" "Tupen"]
    :person/child [{:person/name "David"
                    :person/father "petrid"}
                   {:db/id       "thomasid"
                    :person/name "Thomas"}]}
   {:person/name "Lucy"}
   {:person/name "Elizabeth"}
   {:person/name "Mathew"
    :person/father "thomasid"}
   {:person/name "Eunan"}
   {:person/name "Kerri"}
   {:person/name "Rebecca"}

   {:thing/name "Part A"
    :thing/part [{:thing/name "Part A.A"
                  :thing/part [{:thing/name "Part A.A.A"
                                :thing/part [{:thing/name "Part A.A.A.A"}
                                             {:thing/name "Part A.A.A.B"}]}
                               {:thing/name "Part A.A.B"}]}
                 {:thing/name "Part A.B"
                  :thing/part [{:thing/name "Part A.B.A"
                                :thing/part [{:thing/name "Part A.B.A.A"}
                                             {:thing/name "Part A.B.A.B"}]}]}]}])

(defn new-datomic-conn
  ([] (new-datomic-conn "test"))
  ([name]
   (let [url (str "datomic:mem://" name)]
     (datomic/delete-database url)
     (datomic/create-database url)
     (let [conn (datomic/connect url)]
       @(datomic/transact conn (map datomic-schema-attr test-schema))
       (reset! tmpids (:tempids @(datomic/transact conn test-data)))
       conn))))

(def ^:private test-datomic-db (datomic/db (new-datomic-conn)))

(deftest test-pull-map
  (is (= {:person/name "Petr"}
         (datomic/pull test-datomic-db '[:person/name {:person/child [:not-an/attr]}] [:person/name "Petr"]))))

(deftest test-pull-map-2
  (testing "Map specs can override component expansion"
    (let [parts {:thing/name "Part A"
                 :thing/part [{:thing/name "Part A.A"}
                              {:thing/name "Part A.B"}]}]
      (is (= parts
             (datomic/pull test-datomic-db
                           '[:thing/name {:thing/part [:thing/name]}]
                           [:thing/name "Part A"])))

      (is (= parts
             (datomic/pull test-datomic-db
                           '[:thing/name {:thing/part 1}]
                           [:thing/name "Part A"]))))))

(deftest test-lookup-ref-pull
  (is (= {:person/name "Petr" :person/aka ["Devil" "Tupen"]}
         (datomic/pull test-datomic-db '[:person/name :person/aka] [:person/name "Petr"])))
  (is (= nil
         (datomic/pull test-datomic-db '[:person/name :person/aka] [:person/name "NotInDatabase"])))
  (is (= [nil
          {:person/aka ["Devil" "Tupen"]}
          nil
          nil]
         (datomic/pull-many test-datomic-db
                            '[:person/aka]
                            [[:person/name "Elizabeth"]
                             [:person/name "Petr"]
                             [:person/name "Eunan"]
                             [:person/name "Rebecca"]]))))

(defn -main [& args]
  (let [{:keys [test pass fail error] :as res} (run-tests 'test-datomic.pull-api)]
    (shutdown-agents)
    (when (or (> (+ fail error) 0) (= test 0))
      (System/exit 1))))