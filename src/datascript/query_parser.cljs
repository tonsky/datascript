(ns datascript.query-parser)

;; find-spec                  = ':find' (find-rel | find-coll | find-tuple | find-scalar)
;; find-rel                   = find-elem+
;; find-coll                  = [find-elem '...']
;; find-scalar                = find-elem '.'
;; find-tuple                 = [find-elem+]
;; find-elem                  = (variable | pull-expr | aggregate) 
;; variable                   = symbol starting with "?"
;; aggregate                  = [aggregate-fn-name fn-arg+]
;; fn-arg                     = (variable | constant | src-var)
;; src-var                    = symbol starting with "$"
;; constant                   = any non-variable data literal

(defrecord Variable [symbol])
(defrecord SrcVar   [symbol])
(defrecord BuiltIn  [symbol])
(defrecord Constant [value])
(defrecord FindRel    [elements])
(defrecord FindColl   [element])
(defrecord FindScalar [element])
(defrecord FindTuple  [elements])
(defrecord Aggregate  [fn args])

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

(defn parse-builtin [form]
  (when (and (symbol? form)
             (not (parse-variable form))
             (not (parse-src-var form)))
    (BuiltIn. form))) ;; TODO check actual builtin table

(defn parse-fn-arg [form]
  (or (parse-variable form)
      (parse-constant form)
      (parse-src-var form)))

(defn parse-aggregate [form]
  (when
    (and (sequential? form)
         (>= (count form) 2))
    (let [[fn & args] form
          fn*   (parse-builtin fn)
          args* (parse-seq parse-fn-arg args)]
      (when (and fn* args*)
        (Aggregate. fn* args*)))))

(defn parse-aggregate-custom [form]
  (when
    (and (sequential? form)
         (>= (count form) 3)
         (= (first form) 'aggregate))
    (let [[_ fn & args] form
          fn*   (parse-variable fn)
          args* (parse-seq parse-fn-arg args)]
      (when (and fn* args*)
        (Aggregate. fn* args*)))))

(defn parse-find-elem [form]
  (or (parse-variable form)
      (parse-aggregate-custom form)
      (parse-aggregate form))) ;; TODO or pull-expr

(defn parse-find-rel [form]
  (some->
    (parse-seq parse-find-elem form)
    (FindRel.)))

(defn parse-find-coll [form]
  (when (and (sequential? form)
             (= (count form) 1))
    (let [inner (first form)]
      (when (and (sequential? inner)
                 (= (count inner) 2)
                 (= (second inner) '...))
        (some-> (parse-find-elem (first inner))
                (FindColl.))))))

(defn parse-find-scalar [form]
  (when (and (sequential? form)
             (= (count form) 2)
             (= (second form) '.))
    (some-> (parse-find-elem (first form))
            (FindScalar.))))

(defn parse-find-tuple [form]
  (when (and (sequential? form)
             (= (count form) 1))
    (let [inner (first form)]
      (some->
        (parse-seq parse-find-elem inner)
        (FindTuple.)))))

(defn parse-find [form]
  (or (parse-find-rel form)
      (parse-find-coll form)
      (parse-find-scalar form)
      (parse-find-tuple form)
      (throw (js/Error. "Cannot parse :find, expected: (find-rel | find-coll | find-tuple | find-scalar)"))))

