(ns datascript.test.pull-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.pull-parser :as pp]))

(deftest test-parse-pattern
  (are [pattern expected] (= expected (pp/parse-pull pattern))
    '[:db/id :foo/bar]
    (pp/PullSpec. false {:db/id   {:attr :db/id}
                         :foo/bar {:attr :foo/bar}})
    
    '[(limit :foo 1)]
    (pp/PullSpec. false {:foo {:attr :foo :limit 1}})

    '[* (default :foo "bar")]
    (pp/PullSpec. true {:foo {:attr :foo :default "bar"}})

    '[{:foo ...}]
    (pp/PullSpec. false {:foo {:attr :foo :recursion nil}})

    '[{(limit :foo 2) [:bar :me]}]
    (pp/PullSpec.
     false
     {:foo {:attr :foo
            :limit 2
            :subpattern (pp/PullSpec.
                         false
                         {:bar {:attr :bar}
                          :me {:attr :me}})}})))

(deftest test-parse-bad-limit
  (is
   (thrown? js/Error (pp/parse-pull '[(limit :foo :bar)]))))

(deftest test-parse-bad-default
  (is
   (thrown? js/Error (pp/parse-pull '[(default 1 :bar)]))))

#_(t/test-ns 'datascript.test.pull-parser)
