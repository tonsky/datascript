(ns datascript.test.tuples
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.test.core :as tdc]))

(deftest test-schema
  (let [db (d/empty-db
             {:year+session {:db/tupleAttrs [:year :session]}
              :semester+course+student {:db/tupleAttrs [:semester :course :student]}
              :session+student {:db/tupleAttrs [:session :student]}})]
    (is (= #{:year+session :semester+course+student :session+student}
          (:db.type/tuple (:rschema db))))

    (is (= {:year     {:year+session 0}
            :session  {:year+session 1, :session+student 0}
            :semester {:semester+course+student 0}
            :course   {:semester+course+student 1}
            :student  {:semester+course+student 2, :session+student 1}}
          (:db/attrTuples (:rschema db))))

    (is (thrown-msg? ":t2 :db/tupleAttrs can’t depend on another tuple attribute: :t1"
          (d/empty-db {:t1 {:db/tupleAttrs [:a :b]}
                       :t2 {:db/tupleAttrs [:c :d :e :t1]}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs must be a sequential collection, got: :a"
          (d/empty-db {:t1 {:db/tupleAttrs :a}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs can’t be empty"
          (d/empty-db {:t1 {:db/tupleAttrs ()}})))

    (is (thrown-msg? ":t1 has :db/tupleAttrs, must be :db.cardinality/one"
          (d/empty-db {:t1 {:db/tupleAttrs [:a :b :c]
                            :db/cardinality :db.cardinality/many}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs can’t depend on :db.cardinality/many attribute: :a"
          (d/empty-db {:a  {:db/cardinality :db.cardinality/many}
                       :t1 {:db/tupleAttrs [:a :b :c]}})))))

(deftest test-tx
  (let [conn (d/create-conn {:a+b   {:db/tupleAttrs [:a :b]}
                             :a+c+d {:db/tupleAttrs [:a :c :d]}})]
    (are [tx datoms] (= datoms (tdc/all-datoms (:db-after (d/transact! conn tx))))
      [[:db/add 1 :a "a"]]
      #{[1 :a     "a"]
        [1 :a+b   ["a" nil]]
        [1 :a+c+d ["a" nil nil]]}

      [[:db/add 1 :b "b"]]
      #{[1 :a     "a"]
        [1 :b     "b"]
        [1 :a+b   ["a" "b"]]
        [1 :a+c+d ["a" nil nil]]}

      [[:db/add 1 :a "A"]]
      #{[1 :a     "A"]
        [1 :b     "b"]
        [1 :a+b   ["A" "b"]]
        [1 :a+c+d ["A" nil nil]]}

      [[:db/add 1 :c "c"]
       [:db/add 1 :d "d"]]
      #{[1 :a     "A"]
        [1 :b     "b"]
        [1 :a+b   ["A" "b"]]
        [1 :c     "c"]
        [1 :d     "d"]
        [1 :a+c+d ["A" "c" "d"]]}

      [[:db/add 1 :a "a"]]
      #{[1 :a     "a"]
        [1 :b     "b"]
        [1 :a+b   ["a" "b"]]
        [1 :c     "c"]
        [1 :d     "d"]
        [1 :a+c+d ["a" "c" "d"]]}

      [[:db/add 1 :a "A"]
       [:db/add 1 :b "B"]
       [:db/add 1 :c "C"]
       [:db/add 1 :d "D"]]
      #{[1 :a     "A"]
        [1 :b     "B"]
        [1 :a+b   ["A" "B"]]
        [1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d ["A" "C" "D"]]}

      [[:db/retract 1 :a "A"]]
      #{[1 :b     "B"]
        [1 :a+b   [nil "B"]]
        [1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d [nil "C" "D"]]}

      [[:db/retract 1 :b "B"]]
      #{[1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d [nil "C" "D"]]})))

(deftest test-validation
  (let [db  (d/empty-db {:a+b {:db/tupleAttrs [:a :b]}})
        db1 (d/db-with db [[:db/add 1 :a "a"]])]
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [nil nil]]"
          (d/db-with db [[:db/add 1 :a+b [nil nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
          (d/db-with db1 [[:db/add 1 :a+b ["a" nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
          (d/db-with db [[:db/add 1 :a "a"]
                         [:db/add 1 :a+b ["a" nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/retract 1 :a+b [\"a\" nil]]"
          (d/db-with db1 [[:db/retract 1 :a+b ["a" nil]]])))))