(ns datascript.test.filter
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.core :as tdc]))

(deftest test-filter-db
  (let [db (-> (d/empty-db {:aka { :db/cardinality :db.cardinality/many }})
               (d/db-with [{:db/id 1
                            :name  "Petr"
                            :email "petya@spb.ru"
                            :aka   ["I" "Great"]
                            :password "<SECRET>"}
                           {:db/id 2
                            :name  "Ivan"
                            :aka   ["Terrible" "IV"]
                            :password "<PROTECTED>"}
                           {:db/id 3
                            :name  "Nikolai"
                            :aka   ["II"]
                            :password "<UNKWOWN>"}
                           ]))
        remove-pass (fn [_ datom] (not= :password (:a datom)))
        remove-ivan (fn [_ datom] (not= 2 (:e datom)))
        long-akas   (fn [udb datom] (or (not= :aka (:a datom))
                                        ;; has just 1 aka
                                        (<= (count (:aka (d/entity udb (:e datom)))) 1)
                                        ;; or aka longer that 4 chars
                                        (>= (count (:v datom)) 4)))]
    
    (are [_db _res] (= (d/q '[:find ?v :where [_ :password ?v]] _db) _res)
      db                        #{["<SECRET>"] ["<PROTECTED>"] ["<UNKWOWN>"]}
      (d/filter db remove-pass) #{}
      (d/filter db remove-ivan) #{["<SECRET>"] ["<UNKWOWN>"]}
      (-> db (d/filter remove-ivan) (d/filter remove-pass)) #{})

    (are [_db _res] (= (d/q '[:find ?v :where [_ :aka ?v]] _db) _res)
      db                        #{["I"] ["Great"] ["Terrible"] ["IV"] ["II"]}
      (d/filter db remove-pass) #{["I"] ["Great"] ["Terrible"] ["IV"] ["II"]}
      (d/filter db remove-ivan) #{["I"] ["Great"] ["II"]}
      (d/filter db long-akas)   #{["Great"] ["Terrible"] ["II"]}
      (-> db (d/filter remove-ivan) (d/filter long-akas)) #{["Great"] ["II"]}
      (-> db (d/filter long-akas) (d/filter remove-ivan)) #{["Great"] ["II"]})
     
    (testing "Entities"
      (is (= (:password (d/entity db 1)) "<SECRET>"))
      (is (= (:password (d/entity (d/filter db remove-pass) 1) ::not-found) ::not-found))
      (is (= (:aka (d/entity db 2)) #{"Terrible" "IV"}))
      (is (= (:aka (d/entity (d/filter db long-akas) 2)) #{"Terrible"})))
    
    (testing "Index access"
      (is (= (map :v (d/datoms db :aevt :password))
             ["<SECRET>" "<PROTECTED>" "<UNKWOWN>"]))
      (is (= (map :v (d/datoms (d/filter db remove-pass) :aevt :password))
             [])))
  
    (testing "equiv and hash"
      (is (= (d/db-with db [[:db.fn/retractEntity 2]])
             (d/filter db remove-ivan)))
      (is (= (hash (d/db-with db [[:db.fn/retractEntity 2]]))
             (hash (d/filter db remove-ivan)))))))
