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
