(ns test.datascript.query-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.query-parser :as qp]))

(deftest test-parse-find
  (is (= (qp/parse-find '[?a ?b])
         (qp/FindRel. [(qp/Variable. '?a) (qp/Variable. '?b)])))
  (is (= (qp/parse-find '[[?a ...]])
         (qp/FindColl. (qp/Variable. '?a))))
  (is (= (qp/parse-find '[?a .])
         (qp/FindScalar. (qp/Variable. '?a))))
  (is (= (qp/parse-find '[[?a ?b]])
         (qp/FindTuple. [(qp/Variable. '?a) (qp/Variable. '?b)]))))

(deftest test-parse-aggregate
  (is (= (qp/parse-find '[?a (count ?b)])
         (qp/FindRel. [(qp/Variable. '?a) (qp/Aggregate. (qp/BuiltInAggr. 'count) [(qp/Variable. '?b)])])))
  (is (= (qp/parse-find '[[(count ?a) ...]])
         (qp/FindColl. (qp/Aggregate. (qp/BuiltInAggr. 'count) [(qp/Variable. '?a)]))))
  (is (= (qp/parse-find '[(count ?a) .])
         (qp/FindScalar. (qp/Aggregate. (qp/BuiltInAggr. 'count) [(qp/Variable. '?a)]))))
  (is (= (qp/parse-find '[[(count ?a) ?b]])
         (qp/FindTuple. [(qp/Aggregate. (qp/BuiltInAggr. 'count) [(qp/Variable. '?a)]) (qp/Variable. '?b)]))))

(deftest test-parse-custom-aggregates
  (is (= (qp/parse-find '[(aggregate ?f ?a)])
         (qp/FindRel. [(qp/Aggregate. (qp/Variable. '?f) [(qp/Variable. '?a)])])))
  (is (= (qp/parse-find '[?a (aggregate ?f ?b)])
         (qp/FindRel. [(qp/Variable. '?a) (qp/Aggregate. (qp/Variable. '?f) [(qp/Variable. '?b)])])))
  (is (= (qp/parse-find '[[(aggregate ?f ?a) ...]])
         (qp/FindColl. (qp/Aggregate. (qp/Variable. '?f) [(qp/Variable. '?a)]))))
  (is (= (qp/parse-find '[(aggregate ?f ?a) .])
         (qp/FindScalar. (qp/Aggregate. (qp/Variable. '?f) [(qp/Variable. '?a)]))))
  (is (= (qp/parse-find '[[(aggregate ?f ?a) ?b]])
         (qp/FindTuple. [(qp/Aggregate. (qp/Variable. '?f) [(qp/Variable. '?a)]) (qp/Variable. '?b)]))))

(deftest test-parse-find-elements
  (is (= (qp/parse-find '[(count ?b 1 $x) .])
         (qp/FindScalar. (qp/Aggregate. (qp/BuiltInAggr. 'count)
                                        [(qp/Variable. '?b)
                                         (qp/Constant. 1)
                                         (qp/SrcVar. '$x)])))))

#_(t/test-ns 'test.datascript.query-parser)
