(ns datascript.test.listen
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

(deftest test-listen!
  (let [conn    (d/create-conn)
        reports (atom [])]
    (d/transact! conn [[:db/add -1 :name "Alex"]
                       [:db/add -2 :name "Boris"]])
    (d/listen! conn :test #(swap! reports conj %))
    (d/transact! conn [[:db/add -1 :name "Dima"]
                       [:db/add -1 :age 19]
                       [:db/add -2 :name "Evgeny"]] {:some-metadata 1})
    (d/transact! conn [[:db/add -1 :name "Fedor"]
                       [:db/add 1 :name "Alex2"]         ;; should update
                       [:db/retract 2 :name "Not Boris"] ;; should be skipped
                       [:db/retract 4 :name "Evgeny"]])
    (d/unlisten! conn :test)
    (d/transact! conn [[:db/add -1 :name "Geogry"]])
    
    (is (= (:tx-data (first @reports))
           [(db/datom 3 :name "Dima"   (+ d/tx0 2) true)
            (db/datom 3 :age 19        (+ d/tx0 2) true)
            (db/datom 4 :name "Evgeny" (+ d/tx0 2) true)]))
    (is (= (:tx-meta (first @reports))
           {:some-metadata 1}))
    (is (= (:tx-data (second @reports))
           [(db/datom 5 :name "Fedor"  (+ d/tx0 3) true)
            (db/datom 1 :name "Alex"   (+ d/tx0 3) false)  ;; update -> retract
            (db/datom 1 :name "Alex2"  (+ d/tx0 3) true)   ;;         + add
            (db/datom 4 :name "Evgeny" (+ d/tx0 3) false)]))
    (is (= (:tx-meta (second @reports))
           nil))
    ))
