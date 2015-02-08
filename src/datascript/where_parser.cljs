(ns datascript.where-parser
  (:require
    [clojure.set :as set]
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

(declare parse-clause)

(defrecord Pattern   [source pattern])
(defrecord Predicate [fn args])
(defrecord Function  [fn args binding])
(defrecord RuleExpr  [source name args]) ;; TODO rule with constant or '_' as argument
(defrecord Not       [source vars clauses])
(defrecord Or        [source rule-vars clauses])
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

(defn- collect-vars
  ([form]
    (->> (collect-vars [] form)
         distinct vec))
  ([acc form]
    (cond
      (instance? Not form)         (into acc (.-vars form))
      (instance? Or form)          (collect-vars acc (.-rule-vars form))
      (instance? dp/Variable form) (conj acc form)
      (seqable? form)              (reduce collect-vars acc form)
      :else acc)))

(defn- validate-join-vars [vars clauses form]
  (let [undeclared (set/difference (set vars) (set (collect-vars clauses)))]
    (when-not (empty? undeclared)
      (raise "Join variable not declared inside clauses: " (mapv :symbol undeclared)
             {:error :parser/where, :form form})))
  (when (empty? vars)
    (raise "Join variables should not be empty"
           {:error :parser/where, :form form})))

(defn- validate-not [clause form]
  (validate-join-vars (.-vars clause) (.-clauses clause) form)
  clause)

(defn parse-not [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym & clauses] next-form]
      (when (= 'not sym)
        (if-let [clauses* (dp/parse-seq parse-clause clauses)]
          (-> (Not. source* (collect-vars clauses*) clauses*)
              (validate-not form))
          (raise "Cannot parse 'not' clause, expected [ src-var? 'not' clause+ ]"
                 {:error :parser/where, :form form}))))))

(defn parse-not-join [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym vars & clauses] next-form]
      (when (= 'not-join sym)
        (let [vars*    (dp/parse-seq dp/parse-variable vars)
              clauses* (dp/parse-seq parse-clause clauses)]
          (if (and vars* clauses*)
            (-> (Not. source* vars* clauses*)
                (validate-not form))
            (raise "Cannot parse 'not-join' clause, expected [ src-var? 'not-join' [variable+] clause+ ]"
                   {:error :parser/where, :form form})))))))

(defn validate-or [clause form]
  (let [{{required :required
          free     :free} :rule-vars
         clauses          :clauses} clause
        vars (concat required free)]
    (doseq [clause clauses]
      (validate-join-vars vars [clause] form))
    clause))

(defn parse-and [form]
  (when (and (sequential? form)
             (= 'and (first form)))
    (let [clauses* (dp/parse-seq parse-clause (next form))]
      (if (not-empty clauses*)
        (And. clauses*)
        (raise "Cannot parse 'and' clause, expected [ 'and' clause+ ]"
               {:error :parser/where, :form form})))))

(defn parse-or [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym & clauses] next-form]
      (when (= 'or sym)
        (if-let [clauses* (dp/parse-seq (some-fn parse-and parse-clause) clauses)]
          (-> (Or. source* (dp/RuleVars. nil (collect-vars clauses*)) clauses*)
              (validate-or form))
          (raise "Cannot parse 'or' clause, expected [ src-var? 'or' clause+ ]"
                 {:error :parser/where, :form form}))))))

(defn parse-or-join [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym vars & clauses] next-form]
      (when (= 'or-join sym)
        (let [vars*    (dp/parse-rule-vars vars)
              clauses* (dp/parse-seq parse-clause clauses)]
          (if (and vars* clauses*)
            (-> (Or. source* vars* clauses*)
                (validate-or form))
            (raise "Cannot parse 'or-join' clause, expected [ src-var? 'or-join' [variable+] clause+ ]"
                   {:error :parser/where, :form form})))))))


(defn parse-clause [form]
  (or 
      (parse-not       form)
      (parse-not-join  form)
      (parse-or        form)
      (parse-or-join   form)
      (parse-pred      form)
      (parse-fn        form)
      (parse-rule-expr form)
      (parse-pattern   form)
      (raise "Cannot parse clause, expected (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)"
             {:error :parser/where, :form form} )))

(defn parse-where [form]
  (or (dp/parse-seq parse-clause form)
      (raise "Cannot parse :where clause, expected [clause+]"
             {:error :parser/where, :form form})))
