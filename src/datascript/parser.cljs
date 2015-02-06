(ns datascript.parser
  (:require-macros
    [datascript :refer [raise]]))

(defn of-size? [form size]
  (and (sequential? form)
       (= (count form) size)))

;; src-var                    = symbol starting with "$"
;; variable                   = symbol starting with "?"
;; plain-symbol               = symbol that does not begin with "$" or "?"
;; constant                   = any non-variable data literal
;; fn-arg                     = (variable | constant | src-var)
;; rules-var                  = the symbol "%"
;; rule-vars                  = [ variable+ | ([ variable+ ] variable*) ]

(defrecord Placeholder [])
(defrecord Variable    [symbol])
(defrecord SrcVar      [symbol])
(defrecord DefaultSrc  [])
(defrecord Constant    [value])
(defrecord PlainSymbol [symbol])
(defrecord RuleVars    [required free])

(defn parse-seq [parse-el form]
  (when (sequential? form)
    (reduce #(if-let [parsed (parse-el %2)]
               (conj %1 parsed)
               (reduced nil))
            [] form)))

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

(defn parse-constant [form]
  (when (not (symbol? form))
    (Constant. form)))

(defn parse-plain-symbol [form]
  (when (and (symbol? form)
             (not (parse-variable form))
             (not (parse-src-var form))
             (not (parse-placeholder form)))
    (PlainSymbol. form)))

(defn parse-fn-arg [form]
  (or (parse-variable form)
      (parse-constant form)
      (parse-src-var form)))

(defn parse-rule-vars [form]
  (if (sequential? form)
    (let [[required rest] (if (sequential? (first form))
                            [(first form) (next form)]
                            [nil form])
          required* (parse-seq parse-variable required)
          free*     (parse-seq parse-variable rest)]
      (when (and (empty? required*) (empty? free*))
        (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
               {:error :parser/generic, :form form}))
      (when-not (apply distinct? (concat required* free*))
        (raise "Rule variables should be distinct"
               {:error :parser/generic, :form form}))
      (RuleVars. required* free*))
    (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
           {:error :parser/generic, :form form})))
