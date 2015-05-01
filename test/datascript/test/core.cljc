(ns datascript.test.core
  (:require #?(:cljs [cemerick.cljs.test :as t :refer-macros [is are deftest testing]]
               :clj  [clojure.test :as t :refer [is are deftest testing]])
            #?(:clj [clojure.stacktrace :refer [print-cause-trace]])
            [datascript :as d]
            [datascript.core :as dc]
            [datascript.impl.entity :as de])
  #?(:clj (:import [clojure.lang ExceptionInfo]
                   [java.lang Throwable])))

#?(:cljs (do
(enable-console-print!)
(def Throwable js/Error)))

;; Added special case for printing ex-data of ExceptionInfo
(defmethod t/report :error [{:keys [test-env] :as m}]
  #?(:cljs
     (t/with-test-out test-env
       (t/inc-report-counter test-env :error)
       (println "\nERROR in" (t/testing-vars-str m))
       (when (seq (::test-contexts @test-env))
         (println (t/testing-contexts-str test-env)))
       (when-let [message (:message m)]
         (println message))
       (println "expected:" (pr-str (:expected m)))
       (print "  actual: ")
       (let [actual (:actual m)]
         (cond
           (instance? ExceptionInfo actual) (println (.-stack actual) "\n" (ex-data actual))
           (instance? Throwable actual)     (println (.-stack actual))
           :else                            (prn actual))))
     :clj
     (t/with-test-out
       (t/inc-report-counter :error)
       (println "\nERROR in" (t/testing-vars-str m))
       (when (seq t/*testing-contexts*) (println (t/testing-contexts-str)))
       (when-let [message (:message m)] (println message))
       (println "expected:" (pr-str (:expected m)))
       (print "  actual: ")
       (let [actual (:actual m)]
         (cond
           (instance? ExceptionInfo actual) (do (print-cause-trace actual t/*stack-trace-depth*)
                                                (println (str "\n" (ex-data actual))))
           (instance? Throwable actual)     (print-cause-trace actual t/*stack-trace-depth*)
           :else                            (prn actual))))))
;; utils

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/postwalk #(if (de/entity? %)
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
