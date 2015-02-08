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
  (are [clause res] (= (dwp/parse-clause clause) res)
    '[(pred ?a 1)]
    (dwp/Predicate. (dp/PlainSymbol. 'pred) [(dp/Variable. '?a) (dp/Constant. 1)])
       
    '[(pred)]
    (dwp/Predicate. (dp/PlainSymbol. 'pred) [])
  
    '[(?custom-pred ?a)]
    (dwp/Predicate. (dp/Variable. '?custom-pred) [(dp/Variable. '?a)])
))

(deftest fn
  (are [clause res] (= (dwp/parse-clause clause) res)
    '[(fn ?a 1) ?x]
    (dwp/Function. (dp/PlainSymbol. 'fn) [(dp/Variable. '?a) (dp/Constant. 1)] (dp/BindScalar. (dp/Variable. '?x)))
       
    '[(fn) ?x]
    (dwp/Function. (dp/PlainSymbol. 'fn) [] (dp/BindScalar. (dp/Variable. '?x)))
       
    '[(?custom-fn) ?x]
    (dwp/Function. (dp/Variable. '?custom-fn) [] (dp/BindScalar. (dp/Variable. '?x)))

    '[(?custom-fn ?arg) ?x]
    (dwp/Function. (dp/Variable. '?custom-fn) [(dp/Variable. '?arg)] (dp/BindScalar. (dp/Variable. '?x)))))

(deftest rule-expr
  (are [clause res] (= (dwp/parse-clause clause) res)
    '(friends ?x ?y)
    (dwp/RuleExpr. (dp/DefaultSrc.) (dp/PlainSymbol. 'friends) [(dp/Variable. '?x) (dp/Variable. '?y)])
  
    '(friends "Ivan" _)
    (dwp/RuleExpr. (dp/DefaultSrc.) (dp/PlainSymbol. 'friends) [(dp/Constant. "Ivan") (dp/Placeholder.)])

    '($1 friends ?x ?y)
    (dwp/RuleExpr. (dp/SrcVar. '$1) (dp/PlainSymbol. 'friends) [(dp/Variable. '?x) (dp/Variable. '?y)]))

  (is (thrown-with-msg? ExceptionInfo #"rule-expr requieres at least one argument"
        (dwp/parse-clause '(friends))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse rule-expr arguments"
        (dwp/parse-clause '(friends something)))))

(deftest not-clause
  (are [clause res] (= (dwp/parse-clause clause) res)
    '(not [?e :follows ?x])
    (dwp/Not.
      (dp/DefaultSrc.)
      [(dp/Variable. '?e) (dp/Variable. '?x)]
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)]) ])

    '(not
       [?e :follows ?x]
       [?x _ ?y])
    (dwp/Not.
      (dp/DefaultSrc.)
      [(dp/Variable. '?e) (dp/Variable. '?x) (dp/Variable. '?y)]
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)])
        (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?x) (dp/Placeholder.) (dp/Variable. '?y)])])
       
    '($1 not [?x])
    (dwp/Not.
      (dp/SrcVar. '$1)
      [(dp/Variable. '?x)]
      [ (dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?x)]) ])
       
    '(not-join [?e ?y]
       [?e :follows ?x]
       [?x _ ?y])
    (dwp/Not.
      (dp/DefaultSrc.)
      [(dp/Variable. '?e) (dp/Variable. '?y)]
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)])
        (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?x) (dp/Placeholder.) (dp/Variable. '?y)])])
       
    '($1 not-join [?e] [?e :follows ?x])
    (dwp/Not.
      (dp/SrcVar. '$1)
      [(dp/Variable. '?e)]
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)]) ])
  )
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?x\]"
        (dwp/parse-clause '(not-join [?x] [?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variables should not be empty"
        (dwp/parse-clause '(not-join [] [?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variables should not be empty"
        (dwp/parse-clause '(not [_]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse 'not-join' clause"
        (dwp/parse-clause '(not-join [?x]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse 'not' clause"
      (dwp/parse-clause '(not))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?y\]"
        (dwp/parse-clause '(not-join [?y]
                             (not-join [?x]
                               [?x :follows ?y])))))
)

(deftest or-clause
  (are [clause res] (= (dwp/parse-clause clause) res)
    '(or [?e :follows ?x])
    (dwp/Or.
      (dp/DefaultSrc.)
      (dp/RuleVars. nil [(dp/Variable. '?e) (dp/Variable. '?x)])
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)]) ])

    '(or
       [?e :follows ?x]
       [?e :friend ?x])
    (dwp/Or.
      (dp/DefaultSrc.)
      (dp/RuleVars. nil [(dp/Variable. '?e) (dp/Variable. '?x)])
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)])
        (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :friend) (dp/Variable. '?x)])])
       
    '(or
       [?e :follows ?x]
       (and
         [?e :friend ?x]
         [?x :friend ?e]))
    (dwp/Or.
      (dp/DefaultSrc.)
      (dp/RuleVars. nil [(dp/Variable. '?e) (dp/Variable. '?x)])
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)])
        (dwp/And.
          [(dwp/Pattern.
             (dp/DefaultSrc.)
             [(dp/Variable. '?e) (dp/Constant. :friend) (dp/Variable. '?x)])
           (dwp/Pattern.
             (dp/DefaultSrc.)
             [(dp/Variable. '?x) (dp/Constant. :friend) (dp/Variable. '?e)])]) ])
       
    '($1 or [?x])
    (dwp/Or.
      (dp/SrcVar. '$1)
      (dp/RuleVars. nil [(dp/Variable. '?x)])
      [ (dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?x)]) ])
       
    '(or-join [?e]
       [?e :follows ?x]
       [?e :friend ?y])
    (dwp/Or.
      (dp/DefaultSrc.)
      (dp/RuleVars. nil [(dp/Variable. '?e)])
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)])
        (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :friend) (dp/Variable. '?y)])])
       
    '($1 or-join [[?e] ?x]
         [?e :follows ?x])
    (dwp/Or.
      (dp/SrcVar. '$1)
      (dp/RuleVars. [(dp/Variable. '?e)] [(dp/Variable. '?x)])
      [ (dwp/Pattern.
          (dp/DefaultSrc.)
          [(dp/Variable. '?e) (dp/Constant. :follows) (dp/Variable. '?x)]) ])
  )
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?x\]"
        (dwp/parse-clause '(or-join [?x] [?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?y\]"
        (dwp/parse-clause '(or [?x] [?x ?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?y\]"
        (dwp/parse-clause '(or [?x] [?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?x\]"
        (dwp/parse-clause '(or-join [?x ?y] [?x ?y] [?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse rule-vars"
        (dwp/parse-clause '(or-join [] [?y]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variables should not be empty"
        (dwp/parse-clause '(or [_]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse 'or-join' clause"
        (dwp/parse-clause '(or-join [?x]))))
  
  (is (thrown-with-msg? ExceptionInfo #"Cannot parse 'or' clause"
      (dwp/parse-clause '(or))))
  
  (is (thrown-with-msg? ExceptionInfo #"Join variable not declared inside clauses: \[\?y\]"
        (dwp/parse-clause '(or-join [?y]
                             (or-join [?x]
                               [?x :follows ?y])))))
)

  
