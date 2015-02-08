(ns datascript.parser
  (:require-macros
    [datascript :refer [raise]]))

;; src-var        = symbol starting with "$"
;; variable       = symbol starting with "?"
;; plain-symbol   = symbol that does not begin with "$" or "?"
;; constant       = any non-variable data literal
;; fn-arg         = (variable | constant | src-var)
;; rules-var      = the symbol "%"
;; rule-vars      = [ variable+ | ([ variable+ ] variable*) ]

;; binding        = (bind-scalar | bind-tuple | bind-coll | bind-rel)
;; bind-scalar    = variable
;; bind-tuple     = [ (binding | '_')+ ]
;; bind-coll      = [ binding '...' ]
;; bind-rel       = [ [ (binding | '_')+ ] ]

;; in             = [ (src-var | rules-var | binding)+ ]
;; with           = [ variable+ ]

(defrecord Placeholder [])
(defrecord Variable    [symbol])
(defrecord SrcVar      [symbol])
(defrecord RulesVar    [])
(defrecord DefaultSrc  [])
(defrecord Constant    [value])
(defrecord PlainSymbol [symbol])
(defrecord RuleVars    [required free])

(defrecord BindIgnore [])
(defrecord BindScalar [symbol])
(defrecord BindTuple  [bindings])
(defrecord BindColl   [binding])

(declare of-size? parse-seq)

(defn parse-placeholder [form]
  (when (= '_ form)
    (Placeholder.)))

(defn parse-variable [form]
  (when (and (symbol? form)
             (= (first (name form)) "?"))
    (Variable. form)))

(defn parse-src-var [form]
  (when (and (symbol? form)
             (= (first (name form)) "$"))
    (SrcVar. form)))

(defn parse-rules-var [form]
  (when (= '% form)
    (RulesVar.)))

(defn parse-constant [form]
  (when (not (symbol? form))
    (Constant. form)))

(defn parse-plain-symbol [form]
  (when (and (symbol? form)
             (not (parse-variable form))
             (not (parse-src-var form))
             (not (parse-placeholder form)))
    (PlainSymbol. form)))

;; fn-arg

(defn parse-fn-arg [form]
  (or (parse-variable form)
      (parse-constant form)
      (parse-src-var form)))

;; rule-vars

(defn parse-rule-vars [form]
  (if (sequential? form)
    (let [[required rest] (if (sequential? (first form))
                            [(first form) (next form)]
                            [nil form])
          required* (parse-seq parse-variable required)
          free*     (parse-seq parse-variable rest)]
      (when (and (empty? required*) (empty? free*))
        (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
               {:error :parser/rule-vars, :form form}))
      (when-not (apply distinct? (concat required* free*))
        (raise "Rule variables should be distinct"
               {:error :parser/rule-vars, :form form}))
      (RuleVars. required* free*))
    (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
           {:error :parser/rule-vars, :form form})))

(defn flatten-rule-vars [rule-vars]
  (concat
    (when (.-required rule-vars)
      [(mapv :symbol (.-required rule-vars))]
      (mapv :symbol (.-free rule-vars)))))

(defn rule-vars-arity [rule-vars]
  [(count (.-required rule-vars)) (count (.-free rule-vars))])

;; bindings

(declare parse-binding)

(defn parse-bind-ignore [form]
  (when (= '_ form)
    (BindIgnore.)))

(defn parse-bind-scalar [form]
  (when-let [var (parse-variable form)]
    (BindScalar. var)))

(defn parse-bind-coll [form]
  (when (and (of-size? form 2)
             (= (second form) '...))
    (if-let [sub-bind (parse-binding (first form))]
      (BindColl. sub-bind)
      (raise "Cannot parse collection binding"
             {:error :parser/binding, :form form}))))

(defn parse-tuple-el [form]
  (or (parse-bind-ignore form)
      (parse-binding form)))

(defn parse-bind-tuple [form]
  (when-let [sub-bindings (parse-seq parse-tuple-el form)]
    (if-not (empty? sub-bindings)
      (BindTuple. sub-bindings)
      (raise "Tuple binding cannot be empty"
             {:error :parser/binding, :form form}))))

(defn parse-bind-rel [form]
  (when (and (of-size? form 1)
             (sequential? (first form)))
    ;; relation is just a sequence of tuples
    (BindColl. (parse-bind-tuple (first form)))))

(defn parse-binding [form]
  (or (parse-bind-coll form)
      (parse-bind-rel form)
      (parse-bind-tuple form)
      (parse-bind-ignore form)
      (parse-bind-scalar form)
      (raise "Cannot parse binding, expected (bind-scalar | bind-tuple | bind-coll | bind-rel)"
             {:error :parser/binding, :form form})))

;; in

(defn- parse-in-binding [form]
  (if-let [var (or (parse-src-var form)
                   (parse-rules-var form))]
    (BindScalar. var)
    (parse-binding form)))

(defn parse-in [form]
  (or
    (parse-seq parse-in-binding form)
    (raise "Cannot parse :in clause, expected (src-var | % | bind-scalar | bind-tuple | bind-coll | bind-rel)"
           {:error :parser/in, :form form})))

;; with

(defn parse-with [form]
  (or
    (parse-seq parse-variable form)
    (raise "Cannot parse :with clause, expected [ variable+ ]"
           {:error :parser/with, :form form})))

;; utils

(defn of-size? [form size]
  (and (sequential? form)
       (= (count form) size)))

(defn parse-seq [parse-el form]
  (when (sequential? form)
    (reduce #(if-let [parsed (parse-el %2)]
               (conj %1 parsed)
               (reduced nil))
            [] form)))

(defn collect [pred form & [acc]]
  (let [acc (or acc [])]
    (cond
      (pred form)     (conj acc form)
      (seqable? form) (reduce (fn [acc form] (collect pred form acc)) acc form)
      :else acc)))

(defn postwalk [f form]
  (cond
    ;; additional handling for maps and records that keeps structure type
    (map? form)  (f (reduce (fn [form [k v]] (assoc form k (postwalk f v))) form form))
    (seq? form)  (f (map #(postwalk f %) form))
    (coll? form) (f (into (empty form) (map #(postwalk f %) form)))
    :else        (f form)))
