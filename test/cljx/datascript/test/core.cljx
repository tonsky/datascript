(ns datascript.test.core
  #+cljs
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    #+cljs [cemerick.cljs.test :as t]
    #+clj [clojure.test :as t :refer [is are deftest testing]]
    [datascript :as d]
    [datascript.core :as dc]
    )
  (:import [clojure.lang ExceptionInfo])
 )

#+cljs
(enable-console-print!)

;; test Datom basics
(deftest datom-collection-api
  (let [base (dc/->Datom 1 :2 3 4 5)
        base-hash (hash base)]
    ;; equivalence
    (is    (= base (dc/->Datom 1 :2 3 nil nil)))
    (is (not= base (dc/->Datom 1 :2 nil 4 5)))
    (is (not= base (dc/->Datom 1 nil 3 4 5)))
    (is (not= base (dc/->Datom nil :2 3 4 5)))

    ;; hash equivalence
    (is    (= base-hash (hash (dc/->Datom 1 :2 3 nil nil))))
    (is (not= base-hash (hash (dc/->Datom 1 :2 nil 4 5))))
    (is (not= base-hash (hash (dc/->Datom 1 nil 3 4 5))))
    (is (not= base-hash (hash (dc/->Datom nil :2 3 4 5))))

    ;; lookups
    (is (= (:e base) 1))
    (is (= (:a base) :2))
    (is (= (:v base) 3))
    (is (= (:tx base) 4))
    (is (= (:added base) 5))
    (is (= (:foo base) nil))
    (is (= (:foo base :bar) :bar))

    ;; seq
    (is (= (seq base) '(1 :2 3 4 5)))))


(deftest db-api
  (let [datom0 (dc/->Datom 1 :2 3 4 5)
        datom1 (dc/->Datom 6 :7 8 9 0)
        empty0 (dc/empty-db)
        empty1 (dc/empty-db)]
    (is (   = empty0 empty0))
    (is (   = empty0 empty1))
    (is (not= empty0 nil))
    (is (   = (#'dc/with-datom empty0 datom0) (#'dc/with-datom empty1 datom0)))
    (is (not= (#'dc/with-datom empty0 datom0) (#'dc/with-datom empty0 datom1)))
    (is (not= (#'dc/with-datom empty0 datom0) (#'dc/with-datom empty1 datom1)))
    ))
