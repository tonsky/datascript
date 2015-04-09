(ns datascript.test.serialization
  #+cljs
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  #+cljs
  (:require [cljs.reader :as edn]
            [cemerick.cljs.test :as t])
  #+clj
  (:require [clojure.edn :as edn]
            [clojure.test :as t :refer [is are deftest testing]])
  (:require
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.util :as tdu]))

(defn read-with-tags [s]
  (let [tags {"datascript.core/Datom" d/datom-from-reader
              "datascript.core/DB" d/db-from-reader}]
    #+cljs (binding [edn/*tag-table* (atom tags)] (edn/read-string s))
    #+clj  (edn/read-string {:readers (into {} (for [[k v] tags] [(symbol k) v]))} s)))

(deftest test-pr-read
  (let [d (dc/->Datom 1 :name 3 17 true)]
    (is (= d (read-with-tags (pr-str d)))))
  (let [d (dc/->Datom 1 :name 3 nil nil)]
    (is (= d (read-with-tags (pr-str d)))))

  (let [db (-> (d/empty-db)
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11]]))]
    (is (= db (read-with-tags (pr-str db))))))
