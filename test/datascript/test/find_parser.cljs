(ns datascript.test.find-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.parser :as dp]
    [datascript.find-parser :as dfp]))

(deftest test-parse-find
  (is (= (dfp/parse-find '[?a ?b])
         (dfp/FindRel. [(dp/Variable. '?a) (dp/Variable. '?b)])))
  (is (= (dfp/parse-find '[[?a ...]])
         (dfp/FindColl. (dp/Variable. '?a))))
  (is (= (dfp/parse-find '[?a .])
         (dfp/FindScalar. (dp/Variable. '?a))))
  (is (= (dfp/parse-find '[[?a ?b]])
         (dfp/FindTuple. [(dp/Variable. '?a) (dp/Variable. '?b)]))))

(deftest test-parse-aggregate
  (is (= (dfp/parse-find '[?a (count ?b)])
         (dfp/FindRel. [(dp/Variable. '?a) (dfp/Aggregate. (dp/PlainSymbol. 'count) [(dp/Variable. '?b)])])))
  (is (= (dfp/parse-find '[[(count ?a) ...]])
         (dfp/FindColl. (dfp/Aggregate. (dp/PlainSymbol. 'count) [(dp/Variable. '?a)]))))
  (is (= (dfp/parse-find '[(count ?a) .])
         (dfp/FindScalar. (dfp/Aggregate. (dp/PlainSymbol. 'count) [(dp/Variable. '?a)]))))
  (is (= (dfp/parse-find '[[(count ?a) ?b]])
         (dfp/FindTuple. [(dfp/Aggregate. (dp/PlainSymbol. 'count) [(dp/Variable. '?a)]) (dp/Variable. '?b)]))))

(deftest test-parse-custom-aggregates
  (is (= (dfp/parse-find '[(aggregate ?f ?a)])
         (dfp/FindRel. [(dfp/Aggregate. (dp/Variable. '?f) [(dp/Variable. '?a)])])))
  (is (= (dfp/parse-find '[?a (aggregate ?f ?b)])
         (dfp/FindRel. [(dp/Variable. '?a) (dfp/Aggregate. (dp/Variable. '?f) [(dp/Variable. '?b)])])))
  (is (= (dfp/parse-find '[[(aggregate ?f ?a) ...]])
         (dfp/FindColl. (dfp/Aggregate. (dp/Variable. '?f) [(dp/Variable. '?a)]))))
  (is (= (dfp/parse-find '[(aggregate ?f ?a) .])
         (dfp/FindScalar. (dfp/Aggregate. (dp/Variable. '?f) [(dp/Variable. '?a)]))))
  (is (= (dfp/parse-find '[[(aggregate ?f ?a) ?b]])
         (dfp/FindTuple. [(dfp/Aggregate. (dp/Variable. '?f) [(dp/Variable. '?a)]) (dp/Variable. '?b)]))))

(deftest test-parse-find-elements
  (is (= (dfp/parse-find '[(count ?b 1 $x) .])
         (dfp/FindScalar. (dfp/Aggregate. (dp/PlainSymbol. 'count)
                                          [(dp/Variable. '?b)
                                           (dp/Constant. 1)
                                           (dp/SrcVar. '$x)])))))

#_(t/test-ns 'datascript.test.find-parser)
