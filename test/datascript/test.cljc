(ns datascript.test
  (:require
    [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [is are deftest testing]]
     datascript.test.core
   
     datascript.test.btset
     datascript.test.components
     datascript.test.entity
     datascript.test.explode
     datascript.test.filter
     datascript.test.index
     datascript.test.listen
     datascript.test.lookup-refs
     datascript.test.lru
     datascript.test.parser
     datascript.test.parser-find
     datascript.test.parser-rules
     datascript.test.parser-query
     datascript.test.parser-where
     datascript.test.pull-api
     datascript.test.pull-parser
     datascript.test.query
     datascript.test.query-aggregates
     datascript.test.query-find-specs
     datascript.test.query-fns
     datascript.test.query-pull
     datascript.test.query-rules
     datascript.test.query-v3
     datascript.test.serialization
     datascript.test.transact
     datascript.test.validation
     datascript.test.upsert
   ))

(defn wrap-res [res]
  #?(:cljs (clj->js res)
     :clj  (when (pos? (+ (:fail res) (:error res)))
             (System/exit 1))))

(defn ^:export test-most []
  (wrap-res
    (t/run-all-tests #"datascript\.test\.(?!btset).*")))

(defn ^:export test-btset []
  (wrap-res
    (t/test-ns 'datascript.test.btset)))

(defn ^:export test-all []
  (wrap-res
    (t/run-all-tests)))
