(ns datascript.test.rule-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.parser :as dp]
    [datascript.rule-parser :as drp]
    [datascript.where-parser :as dwp]))

(deftest clauses
  (are [form res] (= (set (drp/parse-rules form)) res)
    '[[(rule ?x)
       [?x :name _]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. nil [(dp/Variable. '?x)])
        [ [(dwp/Pattern.
             (dp/DefaultSrc.)
             [(dp/Variable. '?x) (dp/Constant. :name) (dp/Placeholder.)])] ])})
  
  (is (thrown-with-msg? ExceptionInfo #"Reference to the unknown variable"
        (drp/parse-rules '[[(rule ?x) [?x :name ?y]]]))))

(deftest rule-vars
  (are [form res] (= (set (drp/parse-rules form)) res)       
    '[[(rule [?x] ?y)
       [_]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. [(dp/Variable. '?x)] [(dp/Variable. '?y)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Placeholder.)])] ])}
       
    '[[(rule [?x ?y] ?a ?b)
       [_]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. [(dp/Variable. '?x) (dp/Variable. '?y)] [(dp/Variable. '?a) (dp/Variable. '?b)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Placeholder.)])] ])}
       
    '[[(rule [?x])
       [_]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. [(dp/Variable. '?x)] nil)
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Placeholder.)])] ])})

  (is (thrown-with-msg? ExceptionInfo #"Cannot parse rule-vars"
        (drp/parse-rules '[[(rule) [_]]])))

  (is (thrown-with-msg? ExceptionInfo #"Cannot parse rule-vars"
      (drp/parse-rules '[[(rule []) [_]]])))

  (is (thrown-with-msg? ExceptionInfo #"Rule variables should be distinct"
        (drp/parse-rules '[[(rule ?x ?y ?x) [_]]])))
  
  (is (thrown-with-msg? ExceptionInfo #"Rule variables should be distinct"
        (drp/parse-rules '[[(rule [?x ?y] ?z ?x) [_]]])))
)

(deftest branches
  (are [form res] (= (set (drp/parse-rules form)) res)
    '[[(rule ?x)
       [:a]
       [:b]]
      [(rule ?x)
       [:c]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. nil [(dp/Variable. '?x)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :a)])
           (dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :b)])]
          [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :c)])] ])}
       
    '[[(rule ?x)
       [:a]
       [:b]]
      [(other ?x)
       [:c]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. nil [(dp/Variable. '?x)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :a)])
           (dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :b)])]])
      (drp/Rule.
        (dp/PlainSymbol. 'other)
        (dp/RuleVars. nil [(dp/Variable. '?x)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :c)])] ])}

    ;; renaming vars in same rule branches
    '[[(rule [?x ?y] ?z ?t)
       [?x ?y ?z]
       [?x :name ?t]]
      [(other ?x)
       [:c]]
      [(rule [?a ?b] ?c ?d)
       [?a :name ?d]
       [?b :name ?c]]]
    #{(drp/Rule.
        (dp/PlainSymbol. 'rule)
        (dp/RuleVars. [(dp/Variable. '?x) (dp/Variable. '?y)] 
                      [(dp/Variable. '?z) (dp/Variable. '?t)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?x) (dp/Variable. '?y) (dp/Variable. '?z)])
           (dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?x) (dp/Constant. :name) (dp/Variable. '?t)])]
          [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?x) (dp/Constant. :name) (dp/Variable. '?t)])
           (dwp/Pattern. (dp/DefaultSrc.) [(dp/Variable. '?y) (dp/Constant. :name) (dp/Variable. '?z)])] ])
      (drp/Rule.
        (dp/PlainSymbol. 'other)
        (dp/RuleVars. nil [(dp/Variable. '?x)])
        [ [(dwp/Pattern. (dp/DefaultSrc.) [(dp/Constant. :c)])] ])}
       
  )
  
  (is (thrown-with-msg? ExceptionInfo #"Rule branch should have clauses"
        (drp/parse-rules '[[(rule ?x)]])))
  
  (is (thrown-with-msg? ExceptionInfo #"Arity mismatch"
        (drp/parse-rules '[[(rule ?x) [_]]
                           [(rule ?x ?y) [_]]])))
  
  (is (thrown-with-msg? ExceptionInfo #"Arity mismatch"
        (drp/parse-rules '[[(rule ?x) [_]]
                           [(rule [?x]) [_]]])))
)  
