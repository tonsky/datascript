(ns datascript.rule-parser
  (:require
    [datascript.parser :as dp]
    [datascript.where-parser :as dwp])
  (:require-macros
    [datascript :refer [raise]]))

(defn- postwalk
  [f form]
  (cond
   ;; additional handling for maps and records that keeps structure type
   (map? form)  (f (reduce (fn [form [k v]] (assoc form k (postwalk f v))) form form))
   (seq? form)  (f (map #(postwalk f %) form))
   (coll? form) (f (into (empty form) (map #(postwalk f %) form)))
   :else        (f form)))

;; rule-branch = [rule-head clause+]
;; rule-head   = [rule-name rule-vars]
;; rule-name   = plain-symbol

(defrecord RuleBranch [name vars clauses])
(defrecord Rule [name vars branches])

(defn parse-rule [form]
  (if (sequential? form)
    (let [[head & clauses] form]
      (if (sequential? head)
        (let [[name & vars] head
              name*    (or (dp/parse-plain-symbol name)
                           (raise "Cannot parse rule name, expected plain-symbol"
                                  {:error :parser/rule, :form form}))
              vars*    (dp/parse-rule-vars vars)
              clauses* (or (not-empty (dp/parse-seq dwp/parse-clause clauses))
                           (raise "Rule branch should have clauses"
                                  {:error :parser/rule, :form form}))]
            (RuleBranch. name* vars* clauses*))
        (raise "Cannot parse rule head, expected [rule-name rule-vars]"
               {:error :parser/rule, :form form})))
    (raise "Cannot parse rule, expected [rule-head clause+]"
           {:error :parser/rule, :form form})))

(defn- rename-vars [name from to clauses]
  (if (and (= (count (.-required from)) (count (.-required to)))
           (= (count (.-free from)) (count (.-free to))))
    (let [transform (zipmap (concat (.-required from) (.-free from))
                            (concat (.-required to)   (.-free to)))]
      (postwalk (fn [form]
                  (if (instance? dp/Variable form)
                    (if (contains? transform form)
                      (transform form)
                      (raise "Reference to the unknown variable"
                             {:error :parser/rule, :rule name, :form form}))
                    form))
                clauses))
    (raise "Arity mismatch for rule '" (.-symbol name) "': " from " vs. " to ;; TODO -vars
           {:error :parser/rule, :rule name})))

(defn parse-rules [form]
  (vec
    ;; group rule branches by name
    (for [[name rules] (group-by :name (dp/parse-seq parse-rule form))]
      (let [vars-to (:vars (first rules))
            ;; rename vars in branches so they all have same arguments
            clauses (mapv #(rename-vars name (:vars %) vars-to (:clauses %)) rules)]
        (Rule. name vars-to clauses)))))
