(ns datascript.parser)

;; src-var                    = symbol starting with "$"
;; variable                   = symbol starting with "?"
;; plain-symbol               = symbol that does not begin with "$" or "?"
;; constant                   = any non-variable data literal
;; fn-arg                     = (variable | constant | src-var)
;; rules-var                  = the symbol "%"
;; rule-vars                  = [ variable+ | ([ variable+ ] variable*) ]

(defrecord Variable    [symbol])
(defrecord SrcVar      [symbol])
(defrecord Constant    [value])
(defrecord PlainSymbol [symbol])

(defn parse-seq [parse-el form]
  (when (sequential? form)
    (reduce #(if-let [parsed (parse-el %2)]
               (conj %1 parsed)
               (reduced nil))
            [] form)))

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
             (not (parse-src-var form)))
    (PlainSymbol. form)))

(defn parse-fn-arg [form]
  (or (parse-variable form)
      (parse-constant form)
      (parse-src-var form)))

