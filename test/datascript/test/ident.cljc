(ns datascript.test.ident
  (:require
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]))


(def db
  (-> (d/empty-db {:ref {:db/valueType :db.type/ref}})
      (d/db-with [[:db/add 1 :db/ident :ent1]
                  [:db/add 2 :db/ident :ent2]
                  [:db/add 2 :ref 1]])))


(deftest test-q
  (is (= 1 (d/q '[:find ?v .
                  :where [:ent2 :ref ?v]] db)))
  (is (= 2 (d/q '[:find ?f .
                  :where [?f :ref :ent1]] db))))


(deftest test-transact!
  (let [db' (d/db-with db [[:db/add :ent1 :ref :ent2]])]
    (is (= 2 (-> (d/entity db' :ent1) :ref :db/id)))))


(deftest test-entity
  (is (= {:db/ident :ent1}
         (into {} (d/entity db :ent1)))))


(deftest test-pull
  (is (= {:db/id 1, :db/ident :ent1}
         (d/pull db '[*] :ent1))))


#_(user/test-var #'test-transact!)
#_(t/test-ns 'datascript.test.ident)
