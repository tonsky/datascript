(ns datascript.test.entity
  #+cljs
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  #+clj
  (:require
   [clojure.test :as t :refer [is are deftest testing]])
  #+cljs
  (:require
   [cemerick.cljs.test :as t]
   [cljs.reader :refer [read-string]])
  (:require
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.test.util :as tdu]))

(deftest test-entity
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                           {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}]))
        e  (d/entity db 1)]
    (is (= (:db/id e) 1))
    (is (identical? (d/entity-db e) db))
    (is (= (:name e) "Ivan"))
    (is (= (:age  e) 19))
    (is (= (:aka  e) #{"X" "Y"}))
    (is (= (into {} e)
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 1))
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 2))
           {:name "Ivan", :sex "male", :aka #{"Z"}}))

    (is (= (pr-str (d/entity db 1) "{:db/id 1}")))
    (is (= (pr-str (let [e (d/entity db 1)] (:unknown e) e)) "{:db/id 1}"))
    ;; read back in to account for unordered-ness
    (is (= (read-string (pr-str (let [e (d/entity db 1)] (:name e) e)))
           (read-string "{:name \"Ivan\", :db/id 1}")))))

(deftest test-entity-refs
  (let [db (-> (d/empty-db {:father   {:db/valueType   :db.type/ref}
                            :children {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}})
               (d/db-with
                 [{:db/id 1, :children [10]}
                  {:db/id 10, :father 1, :children [100 101]}
                  {:db/id 100, :father 10}]))
        e  #(d/entity db %)]

    (is (= (:children (e 1))   #{(e 10)}))
    (is (= (:children (e 10))  #{(e 100) (e 101)}))

    (testing "empty attribute"
      (is (= (:children (e 100)) nil)))

    (testing "nested navigation"
      (is (= (-> (e 1) :children first :children) #{(e 100) (e 101)}))
      (is (some #(= (e 10) (:father %)) (-> (e 10) :children)))
      (is (= (-> (e 10) :father :children) #{(e 10)}))

      (testing "after touch"
        (let [e1  (e 1)
              e10 (e 10)]
          (d/touch e1)
          (d/touch e10)
          (is (= (-> e1 :children first :children) #{(e 100) (e 101)}))
          (is (some #(= (e 10) (:father %)) (-> (e 10) :children)))
          (is (= (-> e10 :father :children) #{(e 10)})))))

    (testing "backward navigation"
      (is (= (:_children (e 1))  nil))
      (is (= (:_father   (e 1))  #{(e 10)}))
      (is (= (:_children (e 10)) #{(e 1)}))
      (is (= (:_father   (e 10)) #{(e 100)}))
      (is (= (-> (e 100) :_children first :_children) #{(e 1)}))
    )))
