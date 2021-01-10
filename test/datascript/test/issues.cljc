(ns datascript.test.issues
  (:require
   [datascript.core :as ds]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])))


(deftest ^{:doc "CLJS `apply` + `vector` will hold onto mutable array of arguments directly"}
  issue-262
  (let [db (ds/db-with (ds/empty-db)
             [{:attr "A"} {:attr "B"}])]
    (is (= (ds/q '[:find ?a ?b
                   :where [_ :attr ?a] 
                          [(vector ?a) ?b]]
             db)
          #{["A" ["A"]] ["B" ["B"]]}))))

(deftest ^{:doc "`empty` should preserve meta of db"}
  issue-331
  (let [m  {:foo :bar}
        db (-> (ds/empty-db)
               (with-meta m)
               (empty))]
    (t/is (= m (meta db)))))

#?(:clj
   (deftest ^{:doc "Can't pprint filtered db"}
     issue-330
     (let [base     (-> (ds/empty-db {:aka {:db/cardinality :db.cardinality/many}})
                        (ds/db-with [{:db/id -1
                                      :name  "Maksim"
                                      :age   45
                                      :aka   ["Max Otto von Stierlitz", "Jack Ryan"]}]))
           filtered (ds/filter base (constantly true))]
       (t/is (= (with-out-str (clojure.pprint/pprint base))
                (with-out-str (clojure.pprint/pprint filtered)))))))

(deftest ^{:doc "Can't diff databases with different types of the same attribute"}
  issue-369
  (let [db1 (-> (ds/empty-db)
                (ds/db-with [[:db/add 1 :attr :aa]]))
        db2 (-> (ds/empty-db)
                (ds/db-with [[:db/add 1 :attr "aa"]]))]
    (t/is (= [[(ds/datom 1 :attr :aa)] [(ds/datom 1 :attr "aa")] nil]
             (clojure.data/diff db1 db2)))))

(deftest ^{:doc "Expose a schema as a part of the public API."}
  issue-381
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        db     (ds/empty-db schema)]
    (t/is (= schema (ds/schema db)))))
