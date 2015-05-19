(ns datascript.test.pull-parser
  (:require
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [is are deftest testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.pull-parser :as dpp]
   [datascript.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-parse-pattern
  (are [pattern expected] (= expected (dpp/parse-pull pattern))
    '[:db/id :foo/bar]
    (dpp/->PullSpec false {:db/id   {:attr :db/id}
                         :foo/bar {:attr :foo/bar}})
    
    '[(limit :foo 1)]
    (dpp/->PullSpec false {:foo {:attr :foo :limit 1}})

    '[* (default :foo "bar")]
    (dpp/->PullSpec true {:foo {:attr :foo :default "bar"}})

    '[{:foo ...}]
    (dpp/->PullSpec false {:foo {:attr :foo :recursion nil}})

    '[{(limit :foo 2) [:bar :me]}]
    (dpp/->PullSpec
     false
     {:foo {:attr :foo
            :limit 2
            :subpattern (dpp/->PullSpec
                         false
                         {:bar {:attr :bar}
                          :me {:attr :me}})}})))

(deftest test-parse-bad-limit
  (is
   (thrown? Throwable (dpp/parse-pull '[(limit :foo :bar)]))))

(deftest test-parse-bad-default
  (is
   (thrown? Throwable (dpp/parse-pull '[(default 1 :bar)]))))

#_(t/test-ns 'datascript.test.pull-parser)
