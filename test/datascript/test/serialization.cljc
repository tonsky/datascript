(ns datascript.test.serialization
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [deftest is are testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.test.core :as tdc])
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(defn- read-string* [x]
  #?(:cljs (cljs.reader/read-string x)
     :clj  (clojure.edn/read-string {:readers d/data-readers} x)))

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

#_(deftest test-automatic-data-readers
  ;; failing miserably in CLJ, not sure why. REPL half works, test-clj
  ;; can't find the tag; data_readers may be constrainted for unit
  ;; tests?
  ;; comment out since the reader still fails to parse if only in #_
  ;;(is (= (dc/datom 1 :foo "bar" 1) #datascript/Datom [1 :foo "bar" 1]))
  ;;(is (= (dc/empty-db)             #datascript/DB {:schema nil, :datoms nil}))
  )
