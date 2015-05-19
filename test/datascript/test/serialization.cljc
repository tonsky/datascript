(ns datascript.test.serialization
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [deftest is are testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.test.core :as tdc])
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(defn- read-string* [in]
  #?(:cljs (cljs.reader/read-string in)
     :clj  (let [outs [(clojure.edn/read-string {:readers d/data-readers} in)
                       (binding [*data-readers* d/data-readers] (read-string in))
                       (read-string in)]]
             (assert (apply = outs))
             (first outs))))

(deftest test-pr-read
  (let [d (dc/datom 1 :name 3 17 true)]
    (is (= d (read-string* (pr-str d)))))
  (let [d (dc/datom 1 :name 3 nil nil)]
    (is (= d (read-string* (pr-str d)))))

  (let [db (-> (d/empty-db)
               (d/db-with [ [:db/add 1 :name "Petr"]
                            [:db/add 1 :age 44]
                            [:db/add 2 :name "Ivan"]
                            [:db/add 2 :age 25]
                            [:db/add 3 :name "Sergey"]
                            [:db/add 3 :age 11]]))]
    (is (= db (read-string* (pr-str db))))))

;; confirm (de)-serialization
(deftest test-serialization
  (let [d  (dc/datom 1 :foo "bar" 25 true)
        db (dc/init-db [d] nil)]
    (testing "datom"
      (is (= (pr-str d) "#datascript/Datom [1 :foo \"bar\" 25 true]"))
      (is (= d (dc/datom-from-reader [1 :foo "bar" 25 true]))))
    (testing "db"
      (is (= (pr-str db) "#datascript/DB {:schema nil, :datoms [[1 :foo \"bar\" 25]]}"))
      (is (= db (dc/db-from-reader {:schema nil, :datoms [[1 :foo "bar" 25]]})))
      )))
