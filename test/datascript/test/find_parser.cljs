(ns datascript.test.find-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.find-parser :as fp]))

(deftest test-parse-find
  (is (= (fp/parse-find '[?a ?b])
         (fp/FindRel. [(fp/Variable. '?a) (fp/Variable. '?b)])))
  (is (= (fp/parse-find '[[?a ...]])
         (fp/FindColl. (fp/Variable. '?a))))
  (is (= (fp/parse-find '[?a .])
         (fp/FindScalar. (fp/Variable. '?a))))
  (is (= (fp/parse-find '[[?a ?b]])
         (fp/FindTuple. [(fp/Variable. '?a) (fp/Variable. '?b)]))))

(deftest test-parse-aggregate
  (is (= (fp/parse-find '[?a (count ?b)])
         (fp/FindRel. [(fp/Variable. '?a) (fp/Aggregate. (fp/BuiltInAggr. 'count) [(fp/Variable. '?b)])])))
  (is (= (fp/parse-find '[[(count ?a) ...]])
         (fp/FindColl. (fp/Aggregate. (fp/BuiltInAggr. 'count) [(fp/Variable. '?a)]))))
  (is (= (fp/parse-find '[(count ?a) .])
         (fp/FindScalar. (fp/Aggregate. (fp/BuiltInAggr. 'count) [(fp/Variable. '?a)]))))
  (is (= (fp/parse-find '[[(count ?a) ?b]])
         (fp/FindTuple. [(fp/Aggregate. (fp/BuiltInAggr. 'count) [(fp/Variable. '?a)]) (fp/Variable. '?b)]))))

(deftest test-parse-custom-aggregates
  (is (= (fp/parse-find '[(aggregate ?f ?a)])
         (fp/FindRel. [(fp/Aggregate. (fp/Variable. '?f) [(fp/Variable. '?a)])])))
  (is (= (fp/parse-find '[?a (aggregate ?f ?b)])
         (fp/FindRel. [(fp/Variable. '?a) (fp/Aggregate. (fp/Variable. '?f) [(fp/Variable. '?b)])])))
  (is (= (fp/parse-find '[[(aggregate ?f ?a) ...]])
         (fp/FindColl. (fp/Aggregate. (fp/Variable. '?f) [(fp/Variable. '?a)]))))
  (is (= (fp/parse-find '[(aggregate ?f ?a) .])
         (fp/FindScalar. (fp/Aggregate. (fp/Variable. '?f) [(fp/Variable. '?a)]))))
  (is (= (fp/parse-find '[[(aggregate ?f ?a) ?b]])
         (fp/FindTuple. [(fp/Aggregate. (fp/Variable. '?f) [(fp/Variable. '?a)]) (fp/Variable. '?b)]))))

(deftest test-parse-find-elements
  (is (= (fp/parse-find '[(count ?b 1 $x) .])
         (fp/FindScalar. (fp/Aggregate. (fp/BuiltInAggr. 'count)
                                        [(fp/Variable. '?b)
                                         (fp/Constant. 1)
                                         (fp/SrcVar. '$x)])))))

#_(t/test-ns 'datascript.test.query-parser)
