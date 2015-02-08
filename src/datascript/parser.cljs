(ns datascript.parser
  (:require-macros
    [datascript :refer [raise]]))

(defn of-size? [form size]
  (and (sequential? form)
       (= (count form) size)))

;; src-var        = symbol starting with "$"
;; variable       = symbol starting with "?"
;; plain-symbol   = symbol that does not begin with "$" or "?"
;; constant       = any non-variable data literal
;; fn-arg         = (variable | constant | src-var)
;; rules-var      = the symbol "%"
;; rule-vars      = [ variable+ | ([ variable+ ] variable*) ]

(defrecord Placeholder [])
(defrecord Variable    [symbol])
(defrecord SrcVar      [symbol])
(defrecord RulesVar    [])
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
               {:error :parser/generic, :form form}))
      (when-not (apply distinct? (concat required* free*))
        (raise "Rule variables should be distinct"
               {:error :parser/generic, :form form}))
      (RuleVars. required* free*))
    (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
           {:error :parser/generic, :form form})))

(defn flatten-rule-vars [rule-vars]
  (concat
    (when (.-required rule-vars)
      [(mapv :symbol (.-required rule-vars))]
      (mapv :symbol (.-free rule-vars)))))

(defn rule-vars-arity [rule-vars]
  [(count (.-required rule-vars)) (count (.-free rule-vars))])

;; utils

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
