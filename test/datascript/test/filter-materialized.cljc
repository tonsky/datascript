(ns datascript.test.filter-materialized
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datascript.core :as d]
   [datascript.db :as db]
   [datascript.test.core :as tdc]))

(deftest test-filter-db
  (let [empty-db    (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
        db          (-> empty-db
                        (d/db-with [{:db/id    1
                                     :name     "Petr"
                                     :email    "petya@spb.ru"
                                     :aka      ["I" "Great"]
                                     :password "<SECRET>"}
                                    {:db/id    2
                                     :name     "Ivan"
                                     :aka      ["Terrible" "IV"]
                                     :password "<PROTECTED>"}
                                    {:db/id    3
                                     :name     "Nikolai"
                                     :aka      ["II"]
                                     :password "<UNKWOWN>"}
                                    ]))
        remove-pass (fn [_ datom] (not= :password (:a datom)))
        remove-ivan (fn [_ datom] (not= 2 (:e datom)))
        long-akas   (fn [udb datom] (or (not= :aka (:a datom))
                                        ;; has just 1 aka
                                        (<= (count (:aka (d/entity udb (:e datom)))) 1)
                                        ;; or aka longer that 4 chars
                                        (>= (count (:v datom)) 4)))]

    (are [_db _res] (= _res
                       (d/q '[:find ?v
                              :where [_ :password ?v]]
                            _db))
      db
      #{["<SECRET>"] ["<PROTECTED>"] ["<UNKWOWN>"]}

      (d/filter-materialized db remove-pass)
      #{}

      (d/filter-materialized db remove-ivan)
      #{["<SECRET>"] ["<UNKWOWN>"]}

      (-> db
          (d/filter-materialized remove-ivan)
          (d/filter-materialized remove-pass))
      #{})
    (are [_db _res] (= _res
                       (d/q '[:find ?v
                              :where [_ :aka ?v]]
                            _db))
      db
      #{["I"] ["Great"] ["Terrible"] ["IV"] ["II"]}

      (d/filter-materialized db remove-pass)
      #{["I"] ["Great"] ["Terrible"] ["IV"] ["II"]}

      (d/filter-materialized db remove-ivan)
      #{["I"] ["Great"] ["II"]}

      (d/filter-materialized db long-akas)
      #{["Great"] ["Terrible"] ["II"]}

      (-> db
          (d/filter-materialized remove-ivan)
          (d/filter-materialized long-akas))
      #{["Great"] ["II"]}

      (-> db
          (d/filter-materialized long-akas)
          (d/filter-materialized remove-ivan))
      #{["Great"] ["II"]})

    (testing "Entities"
      (is (= "<SECRET>"
             (:password (d/entity db 1))))
      (is (= ::not-found
             (:password (d/entity (d/filter-materialized db remove-pass) 1) ::not-found)))
      (is (= #{"Terrible" "IV"}
             (:aka (d/entity db 2))))
      (is (= #{"Terrible"}
             (:aka (d/entity (d/filter-materialized db long-akas) 2)))))

    (testing "Index access"
      (is (= ["<SECRET>" "<PROTECTED>" "<UNKWOWN>"]
             (map :v (d/datoms db :aevt :password))))
      (is (= []
             (map :v (d/datoms (d/filter-materialized db remove-pass) :aevt :password)))))

    (testing "equiv"
      (is (= (d/db-with db [[:db.fn/retractEntity 2]])
             (d/filter-materialized db remove-ivan)))
      (is (= empty-db
             (d/filter-materialized empty-db (constantly true))
             (d/filter-materialized db (constantly false)))))

    (testing "hash"
      (is (= (hash (d/db-with db [[:db.fn/retractEntity 2]]))
             (hash (d/filter-materialized db remove-ivan))))
      (is (= (hash empty-db)
             (hash (d/filter-materialized empty-db (constantly true)))
             (hash (d/filter-materialized db (constantly false)))))))

  (testing "double filtering"
    (let [db       (d/db-with (d/empty-db {})
                              [{ :db/id 1, :name "Petr", :age 32}
                               { :db/id 2, :name "Oleg"}
                               { :db/id 3, :name "Ivan", :age 12}])
          has-age? (fn [db datom] (some? (:age (d/entity db (:e datom)))))
          adult?   (fn [db datom] (>= (:age (d/entity db (:e datom))) 18))
          names    (fn [db] (map :v (d/datoms db :aevt :name)))]
      (is (= ["Petr" "Oleg" "Ivan"] (names db)))
      (is (= ["Petr" "Ivan"]        (names (-> db
                                               (d/filter-materialized has-age?)))))
      (is (= ["Petr"]               (names (-> db
                                               (d/filter-materialized has-age?)
                                               (d/filter-materialized adult?))))))))
