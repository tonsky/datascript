(ns datascript.test.datomic-compat
  (:require [clojure.test :refer :all]
            [datomic.api :as datomic]))

(def ^:private test-schema
  {:person/name {:db/unique :db.unique/identity}
   :person/aka  {:db/cardinality :db.cardinality/many}})

(defn- datomic-schema-attr [[name args]]
  (merge
    {:db/ident              name
     :db/valueType          :db.type/string
     :db/cardinality        :db.cardinality/one
     :db.install/_attribute :db.part/db}
    args))

(def ^:private test-datomic-schema
  (mapv datomic-schema-attr test-schema))

(def test-data
  [{:person/name "Petr"
    :person/aka  ["Devil" "Tupen"]}
   {:person/name "David"}
   {:person/name "Thomas"}])

(defn new-datomic-conn
  ([] (new-datomic-conn "test"))
  ([name]
   (let [url (str "datomic:mem://" name)]
     (datomic/delete-database url)
     (datomic/create-database url)
     (let [conn (datomic/connect url)]
       @(datomic/transact conn (map datomic-schema-attr test-schema))
       @(datomic/transact conn test-data)
       conn))))

(def ^:private test-datomic-db (datomic/db (new-datomic-conn)))

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
