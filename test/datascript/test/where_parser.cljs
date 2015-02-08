(ns datascript.test.where-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.parser :as dp]
    [datascript.where-parser :as dwp]))

(deftest pattern
  (are [clause pattern] (= (dwp/parse-clause clause) pattern)
    '[?e ?a ?v]
    (dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?e) (dp/Variable. '?a) (dp/Variable. '?v)])
    
    '[_ ?a _ _]
    (dwp/Pattern. (dp/DefaultSrc.) [(dp/Placeholder.) (dp/Variable. '?a) (dp/Placeholder.) (dp/Placeholder.)])
       
    '[$x _ ?a _ _]
    (dwp/Pattern. (dp/SrcVar. '$x) [(dp/Placeholder.) (dp/Variable. '?a) (dp/Placeholder.) (dp/Placeholder.)])
       
    '[$x _ :name ?v]
    (dwp/Pattern. (dp/SrcVar. '$x) [(dp/Placeholder.) (dp/Constant. :name) (dp/Variable. '?v)]))

    (is (thrown-with-msg? ExceptionInfo #"Pattern could not be empty"
                          (dwp/parse-clause '[])))
)

(deftest pred
  (are [clause pattern] (= (dwp/parse-clause clause) pattern)
    '[[pred ?a 1]]
    (dwp/Predicate. (dp/PlainSymbol. 'pred) [(dp/Variable. '?a) (dp/Constant. 1)])
       
    '[[pred]]
    (dwp/Predicate. (dp/PlainSymbol. 'pred) [])
  
    '[[?custom-pred ?a]]
    (dwp/Predicate. (dp/Variable. '?custom-pred) [(dp/Variable. '?a)])
))

(deftest fn
  (are [clause pattern] (= (dwp/parse-clause clause) pattern)
    '[[fn ?a 1] ?x]
    (dwp/Function. (dp/PlainSymbol. 'fn) [(dp/Variable. '?a) (dp/Constant. 1)] (dp/BindScalar. (dp/Variable. '?x)))
       
    '[[fn] ?x]
    (dwp/Function. (dp/PlainSymbol. 'fn) [] (dp/BindScalar. (dp/Variable. '?x)))
       
    '[[?custom-fn] ?x]
    (dwp/Function. (dp/Variable. '?custom-fn) [] (dp/BindScalar. (dp/Variable. '?x)))

    '[[?custom-fn ?arg] ?x]
    (dwp/Function. (dp/Variable. '?custom-fn) [(dp/Variable. '?arg)] (dp/BindScalar. (dp/Variable. '?x)))))

(deftest rule-expr
  (are [clause pattern] (= (dwp/parse-clause clause) pattern)
    '(friends ?x ?y)
    (dwp/RuleExpr. (dp/DefaultSrc.) (dp/PlainSymbol. 'friends) [(dp/Variable. '?x) (dp/Variable. '?y)]))
  
  (are [clause pattern] (= (dwp/parse-clause clause) pattern)
    '(friends "Ivan" _)
    (dwp/RuleExpr. (dp/DefaultSrc.) (dp/PlainSymbol. 'friends) [(dp/Constant. "Ivan") (dp/Placeholder.)]))

  (are [clause pattern] (= (dwp/parse-clause clause) pattern)
    '($1 friends ?x ?y)
    (dwp/RuleExpr. (dp/SrcVar. '$1) (dp/PlainSymbol. 'friends) [(dp/Variable. '?x) (dp/Variable. '?y)]))

  (is (thrown-with-msg? ExceptionInfo #"rule-expr requieres at least one argument"
        (dwp/parse-clause '(friends))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse rule-expr arguments"
        (dwp/parse-clause '(friends something)))))
