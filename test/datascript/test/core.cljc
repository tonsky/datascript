(ns datascript.test.core
  (:require-macros
    [cemerick.cljs.test :refer [deftest is are testing with-test-out]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript :as d]
    [datascript.core
     #?@(:cljs [:refer-macros [defrecord-updatable-cljs]]
         :clj  [:refer [defrecord-updatable-clj]])]))

(enable-console-print!)

;; Added special case for printing ex-data of ExceptionInfo
(defmethod t/report :error [{:keys [test-env] :as m}]
  (with-test-out test-env
   (t/inc-report-counter test-env :error)
   (println "\nERROR in" (t/testing-vars-str m))
   (when (seq (::test-contexts @test-env))
      (println (t/testing-contexts-str test-env)))
   (when-let [message (:message m)] (println message))
   (println "expected:" (pr-str (:expected m)))
   (print "  actual: ")
   (let [actual (:actual m)]
     (cond
       (instance? ExceptionInfo actual)
         (println (.-stack actual) "\n" (ex-data actual))
       (instance? js/Error actual)
         (println (.-stack actual))
       :else
         (prn actual)))))

;; utils

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/postwalk #(if (instance? datascript.impl.entity/Entity %)
                                     {:db/id (:db/id %)}
                                     %)))))

;; Core tests

(deftest test-protocols
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        db (d/db-with (d/empty-db schema)
                      [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                       {:db/id 2 :name "Petr" :age 37}])]
    (is (= (d/empty-db schema)
           (empty db)))
    (is (= 5 (count db)))
    (is (= (vec (seq db))
           [(d/datom 1 :aka "IV")
            (d/datom 1 :aka "Terrible")
            (d/datom 1 :name "Ivan")
            (d/datom 2 :age 37)
            (d/datom 2 :name "Petr")]))
    ))


;;
;; verify that defrecord-extendable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(#?(:cljs defrecord-updatable-cljs :clj defrecord-updatable-clj)
   HashBeef [x]
  #?@(:cljs [IHash   (-hash  [hb] 0xBEEF)]
      :clj  [IHashEq (hasheq [hb] 0xBEEF)]))

(deftest test-defrecord-extendable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))
