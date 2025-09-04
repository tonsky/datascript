(ns datascript.test.query-deadline
  (:require
   [clojure.test :as t :refer [is are deftest testing]]
   [datascript.core :as d])
  #?(:clj
     (:import
      [clojure.lang ExceptionInfo])))

(deftest timeout
  (is (thrown-with-msg?
       ExceptionInfo
       #"Query and/or pull expression took too long to run."
       (d/q '[:find  ?e1
              :in    $ ?e1 %
              :where (long-query ?e1)
              :timeout 1000]
            []
            1
            '[[(long-query ?e1) [(inc ?e1) ?e1+1] (long-query ?e1+1)]])))
  (is (thrown-with-msg?
       ExceptionInfo
       #"Query and/or pull expression took too long to run."
       (d/q '{:find    [?e1]
              :in      [$ ?e1 %]
              :where   [(long-query ?e1)]
              :timeout 1000}
            []
            1
            '[[(long-query ?e1) [(inc ?e1) ?e1+1] (long-query ?e1+1)]]))))

#?(:clj
   (defn a-fun
     [t]
     (Thread/sleep ^long t)
     1))

#?(:clj
   (deftest deadline-no-cache
     (let [q '[:find  ?r .
               :in $ ?t
               :where [(datascript.test.query-deadline/a-fun ?t) ?r]
               :timeout 1000]]
       (is (thrown-with-msg?
            ExceptionInfo
            #"Query and/or pull expression took too long to run."
            (d/q q [] 2000)))
       ;; if deadline is cached, this will throw too
       (is (= 1 (d/q q [] 10))))))