(ns datascript.find-parser
  (:require
    [datascript.parser :as dp])
  (:require-macros
    [datascript :refer [raise]]))

;; find-spec        = ':find' (find-rel | find-coll | find-tuple | find-scalar)
;; find-rel         = find-elem+
;; find-coll        = [ find-elem '...' ]
;; find-scalar      = find-elem '.'
;; find-tuple       = [ find-elem+ ]
;; find-elem        = (variable | pull-expr | aggregate | custom-aggregate) 
;; pull-expr        = [ 'pull' src-var? variable pull-pattern ]
;; pull-pattern     = (constant | variable)
;; aggregate        = [ aggregate-fn fn-arg+ ]
;; aggregate-fn     = plain-symbol
;; custom-aggregate = [ 'aggregate' variable fn-arg+ ]

(defprotocol IVars
  (-vars [this]))

(extend-protocol IVars
  dp/Variable
  (-vars [this] [(.-symbol this)])
  dp/SrcVar
  (-vars [this] [])
  dp/Constant
  (-vars [this] []))

(defrecord Aggregate [fn args]
  IVars (-vars [_] (-vars (last args))))
(defn aggregate? [element]
  (instance? Aggregate element))

(defrecord Pull [source var pattern]
  IVars (-vars [_] (-vars var)))
(defn pull? [element]
  (instance? Pull element))

(defprotocol IElements
  (-elements [this]))

(defrecord FindRel    [elements]
  IElements (-elements [_] elements))
(defrecord FindColl   [element]
  IElements (-elements [_] [element]))
(defrecord FindScalar [element]
  IElements (-elements [_] [element]))
(defrecord FindTuple  [elements]
  IElements (-elements [_] elements))

(defn elements [find]
  (-elements find))

(defn vars [find]
  (mapcat -vars (-elements find)))


(defn parse-aggregate [form]
  (when (and (sequential? form)
             (>= (count form) 2))
    (let [[fn & args] form
          fn*   (dp/parse-plain-symbol fn)
          args* (dp/parse-seq dp/parse-fn-arg args)]
      (when (and fn* args*)
        (Aggregate. fn* args*)))))

(defn parse-aggregate-custom [form]
  (when (and (sequential? form)
             (= (first form) 'aggregate))
    (if (>= (count form) 3)
      (let [[_ fn & args] form
            fn*   (dp/parse-variable fn)
            args* (dp/parse-seq dp/parse-fn-arg args)]
        (if (and fn* args*)
          (Aggregate. fn* args*)
          (raise "Cannot parse custom aggregate call, expect ['aggregate' variable fn-arg+]"
                 {:error :parser/find, :fragment form})))
      (raise "Cannot parse custom aggregate call, expect ['aggregate' variable fn-arg+]"
             {:error :parser/find, :fragment form}))))

(defn parse-pull-expr [form]
  (when (and (sequential? form)
             (= (first form) 'pull))
    (if (<= 3 (count form) 4)
      (let [long?         (= (count form) 4)
            src           (if long? (nth form 1) '$)
            [var pattern] (if long? (nnext form) (next form))
            src*          (dp/parse-src-var src)                    
            var*          (dp/parse-variable var)
            pattern*      (or (dp/parse-variable pattern)
                              (dp/parse-constant pattern))]
        (if (and src* var* pattern*)
          (Pull. src* var* pattern*)
          (raise "Cannot parse pull expression, expect ['pull' src-var? variable (constant | variable)]"
             {:error :parser/find, :fragment form})))
      (raise "Cannot parse pull expression, expect ['pull' src-var? variable (constant | variable)]"
             {:error :parser/find, :fragment form}))))

(defn parse-find-elem [form]
  (or (dp/parse-variable form)
      (parse-pull-expr form)
      (parse-aggregate-custom form)
      (parse-aggregate form)))

(defn parse-find-rel [form]
  (some->
    (dp/parse-seq parse-find-elem form)
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
        (dp/parse-seq parse-find-elem inner)
        (FindTuple.)))))

(defn parse-find [form]
  (or (parse-find-rel form)
      (parse-find-coll form)
      (parse-find-scalar form)
      (parse-find-tuple form)
      (raise "Cannot parse :find, expected: (find-rel | find-coll | find-tuple | find-scalar)"
             {:error :parser/find, :fragment form})))

