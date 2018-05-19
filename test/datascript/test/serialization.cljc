(ns datascript.test.serialization
  (:require
    [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj
    [clojure.test :as t :refer [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(def readers
  {#?@(:cljs ["cljs.reader/read-string" cljs.reader/read-string]
       :clj  ["clojure.edn/read-string" #(clojure.edn/read-string {:readers d/data-readers} %)
              "clojure.core/read-string" read-string])})

(do db/tx0)

(deftest test-pr-read
  (doseq [[r read-fn] readers]
    (testing r
      (let [d (db/datom 1 0 "Oleg" 15728650 true)]
        (is (= (pr-str d) "#datascript/Datom [1 0 \"Oleg\" 15728650 true]"))
        (is (= d (read-fn (pr-str d)))))

      (let [d (db/datom 1 0 3)]
        (is (= (pr-str d) "#datascript/Datom [1 0 3 16777215 true]"))
        (is (= d (read-fn (pr-str d)))))

      (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity
                                       :db/order 0}
                                :age {:db/order 1}})
                   (d/db-with [[:db/add 1 :name "Petr"]
                               [:db/add 1 :age 44]])
                   (d/db-with [[:db/add 2 :name "Ivan"]]))]
        (is (= (pr-str db)
               (str "#datascript/DB {"
                    ":schema {:name {:db/unique :db.unique/identity, :db/order 0},"
                    " :age {:db/order 1}}, "
                    ":datoms ["
                    "[1 0 \"Petr\" 16777214] "
                    "[1 1 44 16777214] "
                    "[2 0 \"Ivan\" 16777213]"
                    "]}")))
        (is (= db (read-fn (pr-str db))))))))

;(- db/tx0 db/tx-mask)

#?(:clj
   (deftest test-reader-literals
     (is (= #datascript/Datom [1 0 "Oleg"]
            (db/datom 1 0 "Oleg")))
     (is (= #datascript/Datom [1 0 "Oleg" 16777200 false]
            (db/datom 1 0 "Oleg" 16777200 false)))
     ;; not supported because IRecord print method is hard-coded into Compiler
     #_(is (= #datascript/DB {:schema {:name {:db/unique :db.unique/identity :db/order 0}
                                       :age {:db/order 1}}
                              :datoms [[1 0 "Oleg" 100] [1 1 14 100] [2 0 "Petr" 101]]}
              (d/init-db
                [(db/datom 1 0 "Oleg" 100)
                 (db/datom 1 1 14 100)
                 (db/datom 2 0 "Petr" 101)]
                {:name {:db/unique :db.unique/identity :db/order 0}
                 :age {:db/order 1}})))))


(def data
  [[1 0 "Petr"]
   [1 1 "Devil"]
   [1 1 "Tupen"]
   [1 2 15]
   [1 3 2]
   [1 4 "petr@gmail.com"]
   [1 5 10]
   [10 6 "http://"]
   [1 7 {:some-key :some-value}]
   [2 0 "Oleg"]
   [2 2 30]
   [2 4 "oleg@gmail.com"]
   [2 7 [:just :values]]
   [3 0 "Ivan"]
   [3 2 15]
   [3 3 2]
   [3 7 {:another :map}]
   [3 5 30]
   [4 0 "Nick" d/tx0]
   ;; check that facts about transactions doesn’t set off max-eid
   [d/tx0 8 0xdeadbeef]
   [30 6 "https://"]])


(def schema
  {:name    {:db/order 0}                                   ;; nothing special about name
   :aka     {:db/cardinality :db.cardinality/many :db/order 1}
   :age     {:db/index true :db/order 2}
   :follows {:db/valueType :db.type/ref :db/order 3}
   :email   {:db/unique :db.unique/identity :db/order 4}
   :avatar  {:db/valueType :db.type/ref, :db/isComponent true :db/order 5}
   :url     {:db/order 6}                                   ;; just a component prop
   :attach  {:db/order 7}                                   ;; should skip index
   :txInstant {:db/order 8}})

(deftest test-init-db
  (let [db-init (-> (map #(apply d/datom %) data)
                    (d/init-db schema))
        db-transact (->> (map (fn [[e a v]] [:db/add e a v]) data)
                         (d/db-with (d/empty-db schema)))]
    (testing "db-init produces the same result as regular transactions"
      (is (= db-init db-transact)))

    (testing "db-init produces the same max-eid as regular transactions"
      (let [assertions [[:db/add -1 :name "Lex"]]]
        (is (= (d/db-with db-init assertions)
               (d/db-with db-transact assertions)))))

    (testing "Roundtrip"
      (doseq [[r read-fn] readers]
        (testing r
          (is (= db-init (read-fn (pr-str db-init)))))))))
