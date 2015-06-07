(ns datascript.test.serialization
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [deftest is are testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.test.core :as tdc])
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(def readers
  { #?@(:cljs ["cljs.reader/read-string"  cljs.reader/read-string]
        :clj  ["clojure.edn/read-string"  #(clojure.edn/read-string {:readers d/data-readers} %)
               "clojure.core/read-string" read-string]) })

(deftest test-pr-read
  (doseq [[r read-fn] readers]
    (testing r
      (let [d (dc/datom 1 :name "Oleg" 17 true)]
        (is (= (pr-str d) "#datascript/Datom [1 :name \"Oleg\" 17 true]"))
        (is (= d (read-fn (pr-str d)))))
      
      (let [d (dc/datom 1 :name 3)]
        (is (= (pr-str d) "#datascript/Datom [1 :name 3 536870912 true]"))
        (is (= d (read-fn (pr-str d)))))
      
      (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
                   (d/db-with [ [:db/add 1 :name "Petr"]
                                [:db/add 1 :age 44] ])
                   (d/db-with [ [:db/add 2 :name "Ivan"] ]))]
        (is (= (pr-str db)
               (str "#datascript/DB {"
                    ":schema {:name {:db/unique :db.unique/identity}}, "
                    ":datoms ["
                      "[1 :age 44 536870913] "
                      "[1 :name \"Petr\" 536870913] "
                      "[2 :name \"Ivan\" 536870914]"
                    "]}")))
        (is (= db (read-fn (pr-str db))))))))

#?(:clj
  (deftest test-reader-literals
    (is (= #datascript/Datom [1 :name "Oleg"]
                    (dc/datom 1 :name "Oleg")))
    (is (= #datascript/Datom [1 :name "Oleg" 100 false]
                    (dc/datom 1 :name "Oleg" 100 false)))
    ;; not supported because IRecord print method is hard-coded into Compiler
    #_(is (= #datascript/DB {:schema {:name {:db/unique :db.unique/identity}}
                           :datoms [[1 :name "Oleg" 100] [1 :age 14 100] [2 :name "Petr" 101]]}
           (d/init-db 
             [ (dc/datom 1 :name "Oleg" 100)
               (dc/datom 1 :age 14 100)
               (dc/datom 2 :name "Petr" 101) ]
             {:name {:db/unique :db.unique/identity}})))))
