(ns datascript.where-parser
  (:require
    [datascript.parser :as dp])
  (:require-macros
    [datascript :refer [raise]]))


;; clause            = (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)

;; data-pattern      = [ src-var? (variable | constant | '_')+ ]

;; pred-expr         = [ [ pred fn-arg+ ] ]
;; pred              = (plain-symbol | variable)

;; fn-expr           = [ [ fn fn-arg+ ] binding ]
;; fn                = (plain-symbol | variable)

;; rule-expr         = [ src-var? rule-name (variable | constant | '_')+ ]

;; not-clause        = [ src-var? 'not' clause+ ]
;; not-join-clause   = [ src-var? 'not-join' [ variable+ ] clause+ ]

;; or-clause         = [ src-var? 'or' (clause | and-clause)+ ]
;; or-join-clause    = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
;; and-clause        = [ 'and' clause+ ]


(defrecord Pattern   [source pattern])
(defrecord Predicate [fn args])
(defrecord Function  [fn args binding])
(defrecord RuleExpr  [source name args])         ;; TODO rule with constant as argument
(defrecord Not       [source join-vars clauses]) ;; TODO support for default source
(defrecord Or        [source required-join-vars join-vars clauses])
(defrecord And       [clauses])

(defn parse-pattern-el [form]
  (or (dp/parse-placeholder form)
      (dp/parse-variable form)
      (dp/parse-constant form)))

(defn take-source [form]
  (when (sequential? form)
    (if-let [source* (dp/parse-src-var (first form))]
      [source* (next form)]
      [(dp/DefaultSrc.) form])))
      
(defn parse-pattern [form]
  (when-let [[source* next-form] (take-source form)]
    (when-let [pattern* (dp/parse-seq parse-pattern-el next-form)]
      (if-not (empty? pattern*)
        (Pattern. source* pattern*)
        (raise "Pattern could not be empty"
               {:error :parser/where, :form form})))))

(defn parse-call [form]
  (when (sequential? form)
    (let [[fn & args] form
          args  (if (nil? args) [] args)
          fn*   (or (dp/parse-plain-symbol fn)
                    (dp/parse-variable fn))
          args* (dp/parse-seq dp/parse-fn-arg args)]
      (when (and fn* args*)
        [fn* args*]))))

(defn parse-pred [form]
  (when (dp/of-size? form 1)
    (when-let [[fn* args*] (parse-call (first form))]
      (Predicate. fn* args*))))

(defn parse-fn [form]
  (when (dp/of-size? form 2)
    (let [[call binding] form]
      (when-let [[fn* args*] (parse-call call)]
        (when-let [binding* (dp/parse-binding binding)]
          (Function. fn* args* binding*))))))

(defn parse-rule-expr [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[name & args] next-form
          name* (dp/parse-plain-symbol name)
          args* (dp/parse-seq parse-pattern-el args)]
      (when name*
        (cond
          (empty? args)
            (raise "rule-expr requieres at least one argument"
                   {:error :parser/where, :form form})
          (nil? args*)
            (raise "Cannot parse rule-expr arguments, expected [ (variable | constant | '_')+ ]"
                   {:error :parser/where, :form form})
          :else
            (RuleExpr. source* name* args*)
          )))))

(defn parse-clause [form]
  (or 
;;       (parse-not form)
;;       (parse-or form)
      (parse-pred form)
      (parse-fn form)
      (parse-rule-expr form)
      (parse-pattern form)
      (raise "Cannot parse clause, expected (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)"
             {:error :parser/where, :form form} )))

(defn parse-where [form]
  (or (dp/parse-seq parse-clause form)
      (raise "Cannot parse :where clause, expected [clause+]"
             {:error :parser/where, :form form})))
