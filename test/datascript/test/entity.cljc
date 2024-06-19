(ns datascript.test.entity
  (:require
    [clojure.edn :as edn]
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc])
  #?(:clj
     (:import
       [clojure.lang ExceptionInfo])))

(t/use-fixtures :once tdc/no-namespace-maps)

(deftest test-entity
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
             (d/db-with [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                         {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
                         [:db/add 3 :huh? false]]))
        e  (d/entity db 1)]
    (is (= (:db/id e) 1))
    (is (identical? (d/entity-db e) db))
    (is (= (:name e) "Ivan"))
    (is (= (e :name) "Ivan")) ; IFn form
    (is (= (:age  e) 19))
    (is (= (:aka  e) #{"X" "Y"}))
    (is (= true (contains? e :age)))
    (is (= false (contains? e :not-found)))
    (is (= (into {} e)
          {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 1))
          {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 2))
          {:name "Ivan", :sex "male", :aka #{"Z"}}))
    (let [e3 (d/entity db 3)]
      (is (= (into {} e3) {:huh? false})) ; Force caching.
      (is (false? (:huh? e3))))

    (is (= (pr-str (d/entity db 1)) "{:db/id 1}"))
    (is (= (pr-str (let [e (d/entity db 1)] (:unknown e) e)) "{:db/id 1}"))
    ;; read back in to account for unordered-ness
    (is (= (edn/read-string (pr-str (let [e (d/entity db 1)] (:name e) e)))
          (edn/read-string "{:name \"Ivan\", :db/id 1}")))))

(deftest test-entity-refs
  (let [db (-> (d/empty-db {:father   {:db/valueType   :db.type/ref}
                            :children {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}})
             (d/db-with
               [{:db/id 1, :children [10]}
                {:db/id 10, :father 1, :children [100 101]}
                {:db/id 100, :father 10}
                {:db/id 101, :father 10}]))
        e  #(d/entity db %)]

    (is (= (:children (e 1))   #{(e 10)}))
    (is (= (:children (e 10))  #{(e 100) (e 101)}))

    (testing "empty attribute"
      (is (= (:children (e 100)) nil)))

    (testing "nested navigation"
      (is (= (-> (e 1) :children first :children) #{(e 100) (e 101)}))
      (is (= (-> (e 10) :children first :father) (e 10)))
      (is (= (-> (e 10) :father :children) #{(e 10)}))

      (testing "after touch"
        (let [e1  (e 1)
              e10 (e 10)]
          (d/touch e1)
          (d/touch e10)
          (is (= (-> e1 :children first :children) #{(e 100) (e 101)}))
          (is (= (-> e10 :children first :father) (e 10)))
          (is (= (-> e10 :father :children) #{(e 10)})))))

    (testing "backward navigation"
      (is (= (:_children (e 1))  nil))
      (is (= (:_father   (e 1))  #{(e 10)}))
      (is (= (:_children (e 10)) #{(e 1)}))
      (is (= (:_father   (e 10)) #{(e 100) (e 101)}))
      (is (= (-> (e 100) :_children first :_children) #{(e 1)})))))

(deftest test-missing-refs
  (let [schema {:ref       {:db/valueType   :db.type/ref}
                :comp      {:db/valueType   :db.type/ref
                            :db/isComponent true}
                :multiref  {:db/valueType   :db.type/ref
                            :db/cardinality :db.cardinality/many}
                :multicomp {:db/valueType   :db.type/ref
                            :db/isComponent true
                            :db/cardinality :db.cardinality/many}}
        db     (d/empty-db schema)
        db'    (d/db-with db
                 [[:db/add 1 :ref       2]
                  [:db/add 1 :comp      3]
                  [:db/add 1 :multiref  4]
                  [:db/add 1 :multiref  5]
                  [:db/add 1 :multicomp 6]
                  [:db/add 1 :multicomp 6]])]
    (d/touch (d/entity db 1)) ;; does not throw
    (is (= nil (:ref (d/entity db 1))))
    (is (= nil (:comp (d/entity db 1))))
    (is (= nil (:multiref (d/entity db 1))))
    (is (= nil (:multicomp (d/entity db 1))))))
  
(deftest test-entity-misses
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
             (d/db-with [{:db/id 1, :name "Ivan"}
                         {:db/id 2, :name "Oleg"}]))]
    (is (nil? (d/entity db nil)))
    (is (nil? (d/entity db "abc")))
    (is (nil? (d/entity db :keyword)))
    (is (nil? (d/entity db [:name "Petr"])))
    (is (nil? (d/entity db 777)))
    (is (thrown-msg? "Lookup ref attribute should be marked as :db/unique: [:not-an-attr 777]"
          (d/entity db [:not-an-attr 777])))))

(deftest test-entity-equality
  (let [db1 (-> (d/empty-db {})
              (d/db-with [{:db/id 1, :name "Ivan"}]))
        e1  (d/entity db1 1)
        db2 (d/db-with db1 [])
        db3 (d/db-with db2 [{:db/id 2, :name "Oleg"}])]

    (testing "Two entities are equal if they have the same :db/id"
      (is (= e1 e1))
      (is (= e1 (d/entity db1 1)))

      (testing "and refer to the same database"
        (is (not= e1 (d/entity db2 1)))
        (is (not= e1 (d/entity db3 1)))))))

(deftest test-entity-hash
  (let [db1 (-> (d/empty-db {})
              (d/db-with [{:db/id 1, :name "Ivan"}]))
        e1  (d/entity db1 1)
        db2 (d/db-with db1 [])
        db3 (d/db-with db1 [{:db/id 2, :name "Oleg"}])]

    (testing "Two entities have the same hash if they have the same :db/id"
      (is (= (hash e1) (hash e1)))
      (is (= (hash e1) (hash (d/entity db1 1))))

      (testing "and refer to the same database"
        (is (not= (hash e1) (hash (d/entity db2 1))))
        (is (not= (hash e1) (hash (d/entity db3 1))))))))
