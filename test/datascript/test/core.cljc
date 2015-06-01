(ns datascript.test.core
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [deftest is are testing with-test-out]]
   [datascript :as d]
   [datascript.impl.entity :as de]
   [datascript.core :as dc #?@(:cljs [:refer-macros [defrecord-updatable]]
                                     :clj  [:refer [defrecord-updatable]])]))

#?(:cljs
   (enable-console-print!))

;; Added special case for printing ex-data of ExceptionInfo
#?(:cljs
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
           (prn actual))))))

;; utils

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
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
;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  #?@(:cljs [IHash                (-hash  [hb] 0xBEEF)]
      :clj  [clojure.lang.IHashEq (hasheq [hb] 0xBEEF)]))

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))



;; whitebox test to confirm that hash cache caches
(deftest test-db-hash-cache
  (let [db (dc/empty-db)]
    (is (= nil (-> (.-__hash db) #?(:clj (deref)))))
    (let [h (hash db)]
      (is (= h (-> (.-__hash db) #?(:clj (deref))))))))

(defn- now []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(deftest test-uuid
  (loop []
    (when (> (mod (now) 1000) 990) ;; sleeping over end of a second
      (recur)))
  (let [now-ms (now)
        now    (int (/ now-ms 1000))]
    (is (= (* 1000 now) (d/squuid-time-millis (d/squuid))))
    (is (not= (d/squuid) (d/squuid)))
    (is (= (subs (str (d/squuid)) 0 8)
           (subs (str (d/squuid)) 0 8)))))
