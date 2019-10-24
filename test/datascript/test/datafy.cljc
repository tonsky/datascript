(ns datascript.test.datafy
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer [is are deftest testing]])
    [datascript.datafy :as datafy]
    [datascript.core :as d]
    [clojure.core.protocols :as cp]
    [datascript.impl.entity :as e]))

(defn- test-db []
  (let [schema {:ref {:db/valueType :db.type/ref}
                :namespace/ref {:db/valueType :db.type/ref}
                :many/ref {:db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/many}}
        db (-> (d/empty-db schema)
               (d/db-with [{:db/id 1 :name "Parent1"}
                           {:db/id 2 :name "Child1" :ref 1 :namespace/ref 1}
                           {:db/id 3 :name "GrandChild1" :ref 2 :namespace/ref 2}
                           {:db/id 4 :name "Master" :many/ref [1 2 3]}]))]
    db))

(defn- nav [coll k]
  (cp/nav coll k (coll k)))

(defn d+n
  "Helper function to datafy/navigate for a path"
  [coll [k & ks]]
  (if (nil? k)
    coll
    (d+n (nav (cp/datafy coll) k)
         ks)))

(deftest test-navigation
  (let [db (test-db)
        entity (e/entity db 3)]
    (is (= 2 (:db/id (d+n entity [:ref]))))
    (is (= 2 (:db/id (d+n entity [:namespace/ref]))))
    (is (= 1 (:db/id (d+n entity [:ref :namespace/ref]))))
    (is (= 3 (:db/id (d+n entity [:namespace/ref :ref :_ref 0 :namespace/_ref 0]))))
    (is (= #{1 2 3} (set (map :db/id (d+n entity [:many/_ref 0 :many/ref])))))))

