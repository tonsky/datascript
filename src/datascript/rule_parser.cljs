(ns datascript.rule-parser
  (:require
    [clojure.set :as set]
    [datascript.parser :as dp]
    [datascript.where-parser :as dwp])
  (:require-macros
    [datascript :refer [raise]]))

;; rule-branch = [rule-head clause+]
;; rule-head   = [rule-name rule-vars]
;; rule-name   = plain-symbol

(defrecord RuleBranch [vars clauses])
(defrecord Rule [name branches])

(defn validate-vars [vars clauses form]
  (let [declared-vars   (dp/collect #(instance? dp/Variable %) vars #{})
        used-vars       (dp/collect #(instance? dp/Variable %) clauses #{})
        undeclared-vars (set/difference used-vars declared-vars)]
    (when-not (empty? undeclared-vars)
      (raise "Reference to the unknown variables: " (map :symbol undeclared-vars)
             {:error :parser/rule, :form form, :vars undeclared-vars}))))

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
            (validate-vars vars* clauses* form)
            {:name    name*
             :vars    vars*
             :clauses clauses*})
        (raise "Cannot parse rule head, expected [rule-name rule-vars]"
               {:error :parser/rule, :form form})))
    (raise "Cannot parse rule, expected [rule-head clause+]"
           {:error :parser/rule, :form form})))

(defn validate-arity [name branches]
  (let [vars0  (:vars (first branches))
        arity0 (dp/rule-vars-arity vars0)]
    (doseq [b    (next branches)
            :let [vars (:vars b)]]
      (when (not= arity0 (dp/rule-vars-arity vars))
        (raise "Arity mismatch for rule '" (.-symbol name) "': "
               (dp/flatten-rule-vars vars0) " vs. " (dp/flatten-rule-vars vars)
         {:error :parser/rule, :rule name})))))

(defn parse-rules [form]
  (vec
    ;; group rule branches by name
    (for [[name branches] (group-by :name (dp/parse-seq parse-rule form))
          :let [branches (mapv #(RuleBranch. (:vars %) (:clauses %)) branches)]]
      (do
        (validate-arity name branches)
        (Rule. name branches)))))

          
