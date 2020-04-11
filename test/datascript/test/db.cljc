(ns datascript.test.db
  (:require
    [clojure.data]
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                                      :clj  [:refer [defrecord-updatable]])]))

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
  (let [db (db/empty-db)]
    (is (= 0         @(.-hash db)))
    (let [h (hash db)]
      (is (= h @(.-hash db))))))

(defn- now []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ now-ms 1000))]
    (is (= (* 1000 now) (d/squuid-time-millis (d/squuid))))
    (is (not= (d/squuid) (d/squuid)))
    (is (= (subs (str (d/squuid)) 0 8)
           (subs (str (d/squuid)) 0 8)))))

(deftest test-diff
  (is (= [[(d/datom 1 :b 2) (d/datom 1 :c 4) (d/datom 2 :a 1)]
          [(d/datom 1 :b 3) (d/datom 1 :d 5)]
          [(d/datom 1 :a 1)]]
         (clojure.data/diff
           (-> (d/empty-db) (d/db-with [{:a 1 :b 2 :c 4} {:a 1}]))
           (-> (d/empty-db) (d/db-with [{:b 3 :d 5}]) (d/db-with [{:db/id 1 :a 1}]))))))
