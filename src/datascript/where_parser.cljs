(ns datascript.where-parser
  (:require
    [datascript.parser :as dp])
  (:require-macros
    [datascript :refer [raise]]))


;; clause            = (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)

;; data-pattern      = [ src-var? (variable | constant | '_')+ ]

;; pred-expr         = [ src-var? [ pred fn-arg+ ] ]
;; pred              = (plain-symbol | variable)

;; fn-expr           = [ src-var? [ fn fn-arg+ ] binding]
;; fn                = (plain-symbol | variable)
;; binding           = (bind-scalar | bind-tuple | bind-coll | bind-rel)
;; bind-scalar       = variable
;; bind-tuple        = [ variable+ ]
;; bind-coll         = [ variable '...' ]
;; bind-rel          = [ [ variable+ ] ]

;; rule-expr         = [ src-var? rule-name variable+ ]

;; not-clause        = [ src-var? 'not' clause+ ]
;; not-join-clause   = [ src-var? 'not-join' [ variable+ ] clause+ ]

;; or-clause         = [ src-var? 'or' (clause | and-clause)+ ]
;; or-join-clause    = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
;; and-clause        = [ 'and' clause+ ]


