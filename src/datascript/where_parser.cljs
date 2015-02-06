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
;; binding           = (bind-scalar | bind-tuple | bind-coll | bind-rel)
;; bind-scalar       = variable
;; bind-tuple        = [ (binding | '_')+ ]
;; bind-coll         = [ binding '...' ]
;; bind-rel          = [ [ (binding | '_')+ ] ]

;; rule-expr         = [ src-var? rule-name variable+ ]

;; not-clause        = [ src-var? 'not' clause+ ]
;; not-join-clause   = [ src-var? 'not-join' [ variable+ ] clause+ ]

;; or-clause         = [ src-var? 'or' (clause | and-clause)+ ]
;; or-join-clause    = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
;; and-clause        = [ 'and' clause+ ]



(defrecord Pattern    [source pattern])

(defrecord Predicate  [fn args])

(defrecord Function   [fn args binding])
(defrecord BindIgnore [])
(defrecord BindScalar [symbol])
(defrecord BindTuple  [bindings])
(defrecord BindColl   [binding])

;; TODO rule with constant as argument
(defrecord Rule [source name args])

;; TODO support for default source
(defrecord Not [source join-vars clauses])
(defrecord Or  [source required-join-vars join-vars clauses])
(defrecord And [clauses])

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

(declare parse-binding)

(defn parse-bind-ignore [form]
  (when (= '_ form)
    (BindIgnore.)))

(defn parse-bind-scalar [form]
  (when-let [var (dp/parse-variable form)]
    (BindScalar. var)))

(defn parse-bind-coll [form]
  (when (and (dp/of-size? form 2)
             (= (second form) '...))
    (if-let [sub-bind (parse-binding (first form))]
      (BindColl. sub-bind)
      (raise "Cannot parse collection binding"
             {:error :parser/where, :form form}))))

(defn parse-tuple-el [form]
  (or (parse-bind-ignore form)
      (parse-binding form)))

(defn parse-bind-tuple [form]
  (when-let [sub-bindings (dp/parse-seq parse-tuple-el form)]
    (if-not (empty? sub-bindings)
      (BindTuple. sub-bindings)
      (raise "Tuple binding cannot be empty"
             {:error :parser/where, :form form}))))

(defn parse-bind-rel [form]
  (when (and (dp/of-size? form 1)
             (sequential? (first form)))
    ;; relation is just a sequence of tuples
    (BindColl. (parse-bind-tuple (first form)))))

(defn parse-binding [form]
  (or (parse-bind-coll form)
      (parse-bind-rel form)
      (parse-bind-tuple form)
      (parse-bind-scalar form)
      (raise "Cannot parse binding, expected (bind-scalar | bind-tuple | bind-coll | bind-rel)"
             {:error :parser/where, :form form})))

(defn parse-fn [form]
  (when (dp/of-size? form 2)
    (let [[call binding] form]
      (when-let [[fn* args*] (parse-call call)]
        (when-let [binding* (parse-binding binding)]
          (Function. fn* args* binding*))))))

(defn parse-rule [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[name & args] next-form
          name* (dp/parse-plain-symbol name)
          args* (dp/parse-seq dp/parse-variable args)]
      (when name*
        (cond
          (empty? args)
            (raise "Rule requieres at least one variable"
                   {:error :parser/where, :form form})
          (nil? args*)
            (raise "Cannot parse rule args, expected [variable+]"
                   {:error :parser/where, :form form})
          :else
            (Rule. source* name* args*)
          )))))

(defn parse-clause [form]
  (or 
;;       (parse-not form)
;;       (parse-or form)
      (parse-pred form)
      (parse-fn form)
      (parse-rule form)
      (parse-pattern form)
      (raise "Cannot parse clause, expected (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)"
             {:error :parser/where, :form form} )))

(defn parse-where [form]
  (or (dp/parse-seq parse-clause form)
      (raise "Cannot parse :where clause, expected [clause+]"
             {:error :parser/where, :form form})))
