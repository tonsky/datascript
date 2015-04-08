(ns datascript.test.serialization
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [datascript.core :as dc]
    [datascript :as d]
    [cemerick.cljs.test :as t]
    [datascript.test.core :as tdc]))

(deftest test-pr-read
  (binding [cljs.reader/*tag-table* (atom {"datascript/Datom" d/datom-from-reader})]
    (let [d (dc/Datom. 1 :name 3 17 true)]
      (is (= d (cljs.reader/read-string (pr-str d)))))
    (let [d (dc/Datom. 1 :name 3 nil nil)]
      (is (= d (cljs.reader/read-string (pr-str d))))))

  (let [db (-> (d/empty-db)
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11]]))]
    (binding [cljs.reader/*tag-table* (atom {"datascript/DB" d/db-from-reader})]
      (is (= db (cljs.reader/read-string (pr-str db)))))))
