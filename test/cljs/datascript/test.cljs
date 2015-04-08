(ns datascript.test
  (:require
    [cemerick.cljs.test :as t]
     datascript.test.core
   
;;      datascript.test.btset
     datascript.test.components
     datascript.test.entity
     datascript.test.explode
     datascript.test.filter
     datascript.test.index
     datascript.test.listen
     datascript.test.lookup-refs
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
     datascript.test.serialization
     datascript.test.transact
     datascript.test.validation
     datascript.test.upsert
   ))

#_(t/run-all-tests)

(defn ^:export test-all []
  (t/run-all-tests))
