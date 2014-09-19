(ns datascript.query
  (:require
    [cljs.reader]
    [clojure.set :as set]
    [clojure.walk :as walk]
    [datascript.core :as dc]
    [datascript.impl.entity :as de]))


;; Records

(defrecord Context [rels sources rules])
;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])


;; Utilities

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1))
                    (set (keys attrs2))))

(defn concatv [& xs]
  (vec (apply concat xs)))

(defn source? [sym]
  (and (symbol? sym)
       (= \$ (first (name sym)))))

(defn free-var? [sym]
  (and (symbol? sym)
       (= \? (first (name sym)))
       (not= '_ sym)))

(defn- looks-like? [pattern form]
  (cond
    (= '_ pattern)
      true
    (= '[*] pattern)
      (sequential? form)
    (sequential? pattern)
      (and (sequential? form)
           (= (count form) (count pattern))
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector pattern form)))
    (symbol? pattern)
      (= form pattern)
    :else ;; (predicate? pattern)
      (pattern form)))

;; Relation algebra

(defn join-tuples [t1 idxs1 t2 idxs2]
  (let [l1  (alength idxs1)
        l2  (alength idxs2)
        res (js/Array. (+ l1 l2))]
    (dotimes [i l1]
      (aset res i (aget t1 (aget idxs1 i)))) ;; FIXME aget
    (dotimes [i l2]
      (aset res (+ l1 i) (aget t2 (aget idxs2 i)))) ;; FIXME aget
    res))

(defn sum-rel [a b]
  (Relation. (:attrs a) (concat (:tuples a) (:tuples b))))

(defn prod-rel
  ([] (Relation. {} [#js[]]))
  ([rel1 rel2]
    (let [attrs1 (keys (:attrs rel1))
          attrs2 (keys (:attrs rel2))
          idxs1  (to-array (map (:attrs rel1) attrs1))
          idxs2  (to-array (map (:attrs rel2) attrs2))]
      (Relation.
        (zipmap (concat attrs1 attrs2) (range))
        (for [t1 (:tuples rel1)
              t2 (:tuples rel2)]
          (join-tuples t1 idxs1 t2 idxs2))))))

;; built-ins

(defn- -differ? [& xs]
  (let [l (count xs)]
    (not= (take (/ l 2) xs) (drop (/ l 2) xs))))

(defn- -get-else
  [db e a else-val]
  (if-let [datom (first (dc/-search db [e a]))]
    (.-v datom)
    else-val))

(defn- -get-some
  [db e & as]
  (reduce
   (fn [_ a]
     (when-let [datom (first (dc/-search db [e a]))]
       (reduced (.-v datom))))
   nil
   as))

(defn- -missing?
  [db e a]
  (nil? (get (de/entity db e) a)))

(def built-ins {
  '= =, '== ==, 'not= not=, '!= not=, '< <, '> >, '<= <=, '>= >=, '+ +, '- -,
  '* *, '/ /, 'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec, 'max max, 'min min,
  'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'true? true?,
  'false? false?, 'nil? nil?, 'str str, 'identity identity, 'vector vector,
  '-differ? -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?, 'ground identity})

(def built-in-aggregates {
  'distinct (comp vec distinct)
  'min      (fn
              ([coll] (reduce min coll))
              ([n coll]
                (vec
                  (reduce (fn [acc x]
                            (cond
                              (< (count acc) n) (sort (conj acc x))
                              (< x (last acc))  (sort (conj (butlast acc) x))
                              :else             acc))
                          [] coll))))
  'max      (fn
              ([coll] (reduce max coll))
              ([n coll]
                (vec
                  (reduce (fn [acc x]
                            (cond
                              (< (count acc) n) (sort (conj acc x))
                              (> x (first acc)) (sort (conj (next acc) x))
                              :else             acc))
                          [] coll))))
  'sum      #(reduce + 0 %)
  'rand     (fn
              ([coll] (rand-nth coll))
              ([n coll] (vec (repeatedly n #(rand-nth coll)))))
  'sample   (fn [n coll]
              (vec (take n (shuffle coll))))
  'count    count})


;;

(defn in->rel [form value]
  (condp looks-like? form
    '[_ ...] ;; collection binding [?x ...]
      (reduce sum-rel
        (map #(in->rel (first form) %) value))
    '[[*]]   ;; relation binding [[?a ?b]]
      (reduce sum-rel
        (map #(in->rel (first form) %) value))
    '[*]     ;; tuple binding [?a ?b]
      (reduce prod-rel
        (map #(in->rel %1 %2) form value))
    '_       ;; regular binding ?x
      (Relation. {form 0} [#js [value]])))

(defn parse-rules [rules]
  (let [rules (if (string? rules) (cljs.reader/read-string rules) rules)] ;; for datascript.js interop
    (group-by ffirst rules)))

(defn parse-in [context [in value]]
  (cond
    (source? in)
      (update-in context [:sources] assoc in value)
    (= '% in)
      (assoc context :rules (parse-rules value))
    :else
      (update-in context [:rels] conj (in->rel in value))))

(defn parse-ins [context ins values]
  (reduce parse-in context (map vector ins values)))

;;

(defn tuple-key-fn [idxs]
  (if (== (count idxs) 1)
    (let [idx (first idxs)]
      (fn [tuple]
        (aget tuple idx)))
    (let [idxs (to-array idxs)]
      (fn [tuple]
        (list* (.map idxs #(aget tuple %))))))) ;; FIXME aget

(defn hash-attrs [idxs tuples]
  (let [key-fn (tuple-key-fn idxs)]
    (loop [tuples     tuples
           hash-table (transient {})]
      (if-let [tuple (first tuples)]
        (let [key (key-fn tuple)]
          (recur (next tuples)
                 (assoc! hash-table key (conj (get hash-table key '()) tuple))))
        (persistent! hash-table)))))

(defn hash-join [rel1 rel2]
  (let [tuples1       (:tuples rel1)
        tuples2       (:tuples rel2)
        attrs1        (:attrs rel1)
        attrs2        (:attrs rel2)
        common-attrs  (vec (intersect-keys (:attrs rel1) (:attrs rel2)))
        common-idxs1  (map attrs1 common-attrs)
        common-idxs2  (map attrs2 common-attrs)
        keep-attrs1   (keys attrs1)
        keep-attrs2   (vec (set/difference (set (keys attrs2)) (set (keys attrs1))))
        keep-idxs1    (to-array (map attrs1 keep-attrs1))
        keep-idxs2    (to-array (map attrs2 keep-attrs2))
        hash          (hash-attrs common-idxs1 tuples1)
        key-fn        (tuple-key-fn common-idxs2)
        new-tuples    (->>
                        (reduce (fn [acc tuple2]
                                  (let [key (key-fn tuple2)]
                                    (if-let [tuples1 (get hash key)]
                                      (reduce (fn [acc tuple1]
                                                (conj! acc (join-tuples tuple1 keep-idxs1 tuple2 keep-idxs2)))
                                              acc tuples1)
                                      acc)))
                          (transient []) tuples2)
                        (persistent!))]
    (Relation. (zipmap (concat keep-attrs1 keep-attrs2) (range))
               new-tuples)))

(defn lookup-pattern-db [db pattern]
  ;; TODO optimize with bound attrs min/max values here
  (let [search-pattern (mapv #(if (symbol? %) nil %) pattern)
        datoms         (dc/-search db search-pattern)
        attr->prop     (->> (map vector pattern ["e" "a" "v" "tx"])
                            (filter (fn [[s _]] (free-var? s)))
                            (into {}))]
    (Relation. attr->prop datoms)))

(defn matches-pattern? [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (symbol? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll [coll pattern]
  (let [data       (filter #(matches-pattern? pattern %) coll)
        attr->idx  (->> (map vector pattern (range))
                        (filter (fn [[s _]] (free-var? s)))
                        (into {}))]
    (Relation. attr->idx (map to-array data)))) ;; FIXME to-array

(defn lookup-pattern [context clause]
  (let [[source-sym pattern] (if (source? (first clause))
                               [(first clause) (next clause)]
                               ['$ clause])
        source   (get (:sources context) source-sym)]
    (cond
      (instance? dc/DB source)
        (lookup-pattern-db source pattern)
      :else
        (lookup-pattern-coll source pattern))))

(defn collapse-rels [rels new-rel]
  (loop [rels    rels
         new-rel new-rel
         acc     []]
    (if-let [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))


(defn- context-resolve-val [context sym]
  ;; TODO raise if more than one tuple bound
  (when-let [rel (first (filter #(contains? (:attrs %) sym) (:rels context)))]
    (aget (first (:tuples rel)) ((:attrs rel) sym))))

(defn- rel-contains-attrs? [rel attrs]
  (not (empty? (set/intersection (set attrs) (set (keys (:attrs rel)))))))

(defn- rel-prod-by-attrs [context attrs]
  (let [rels       (filter #(rel-contains-attrs? % attrs) (:rels context))
        production (reduce prod-rel rels)]
    [(update-in context [:rels] #(remove (set rels) %)) production]))

(defn -call-fn [context rel f args]
  (fn [tuple]
    ;; TODO raise if not all args are bound
    (let [resolved-args (map #(if (symbol? %)
                                (or
                                 (get (:sources context) %)
                                 (aget tuple (get (:attrs rel) %)))
                                %)
                             args)]
      (apply f resolved-args))))

(defn filter-by-pred [context clause]
  (let [[[f & args]] clause
        pred         (or (get built-ins f)
                         (context-resolve-val context f))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        tuple-pred   (-call-fn context production pred args)
        new-rel      (update-in production [:tuples] #(filter tuple-pred %))]
    (update-in context [:rels] conj new-rel)))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        fun      (or (get built-ins f)
                     (context-resolve-val context f))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        tuple-fn (-call-fn context production fun args)
        new-rel  (->> (:tuples production)
                   (map #(let [val (tuple-fn %)
                               rel (in->rel out val)]
                             (prod-rel (Relation. (:attrs production) [%]) rel)))
                   (reduce sum-rel))]
    (update-in context [:rels] conj new-rel)))



;;; RULES

(defn rule? [context clause]
  (and (sequential? clause)
       (contains? (:rules context)
                  (if (source? (first clause))
                    (second clause)
                    (first clause)))))

(declare -collect)
(declare -resolve-clause)

(def rule-seqid (atom 0))

(defn expand-rule [clause context used-args]
  (let [[rule & call-args] clause
        seqid              (swap! rule-seqid inc)
        branches           (get (:rules context) rule)]
    (for [branch branches
          :let [[[_ & rule-args] & clauses] branch
                replacements (zipmap rule-args call-args)]]
      (walk/postwalk
       #(if (free-var? %)
          (or (replacements %)
              (symbol (str (name %) "__auto__" seqid)))
          %)
        clauses))))

(defn remove-pairs [xs ys]
  (let [pairs (->> (map vector xs ys)
                   (remove (fn [[x y]] (= x y))))]
    [(map first pairs)
     (map second pairs)]))

(defn rule-gen-guards [rule-clause used-args]
  (let [[rule & call-args] rule-clause
        prev-call-args     (get used-args rule)]
    (for [prev-args prev-call-args
          :let [[call-args prev-args] (remove-pairs call-args prev-args)]]
      [(concat ['-differ?] call-args prev-args)])))

(defn walk-collect [form pred]
  (let [res (atom [])]
    (walk/postwalk #(do (when (pred %) (swap! res conj %)) %) form)
    @res))

(defn split-guards [clauses guards]
  (let [bound-vars (set (walk-collect clauses free-var?))
        pred       (fn [[[_ & vars]]] (every? bound-vars vars))]
    [(filter pred guards)
     (remove pred guards)]))

(defn solve-rule [context clause]
  (let [final-attrs     (filter free-var? clause)
        final-attrs-map (zipmap final-attrs (range))
;;         clause-cache    (atom {}) ;; TODO
        solve           (fn [prefix-context clauses]
                          (reduce -resolve-clause prefix-context clauses))
        empty-rels?     (fn [context]
                          (some #(empty? (:tuples %)) (:rels context)))]
    (loop [stack (list {:prefix-clauses []
                        :prefix-context (assoc context :rels [])
                        :clauses        [clause]
                        :used-args      {}
                        :pending-guards {}})
           rel   (Relation. final-attrs-map [])]
      (if-let [frame (first stack)]
        (let [[clauses [rule-clause & next-clauses]] (split-with #(not (rule? context %)) (:clauses frame))]
          (if (nil? rule-clause)

            ;; no rules --> expand, collect, sum
            (let [context (solve (:prefix-context frame) clauses)
                  tuples  (-collect context final-attrs)
                  new-rel (Relation. final-attrs-map tuples)]
              (recur (next stack) (sum-rel rel new-rel)))

            ;; has rule --> add guards --> check if dead --> expand rule --> push to stack, recur
            (let [[rule & call-args]     rule-clause
                  guards                 (rule-gen-guards rule-clause (:used-args frame))
                  [active-gs pending-gs] (split-guards (concat (:prefix-clauses frame) clauses)
                                                       (concat guards (:pending-guards frame)))]
              (if (some #(= % '[(-differ?)]) active-gs) ;; trivial always false case like [(not= [?a ?b] [?a ?b])]

                ;; this branch has no data, just drop it from stack
                (recur (next stack) rel)

                (let [prefix-clauses (concat clauses active-gs)
                      prefix-context (solve (:prefix-context frame) prefix-clauses)]
                  (if (empty-rels? prefix-context)

                    ;; this branch has no data, just drop it from stack
                    (recur (next stack) rel)

                    ;; need to expand rule to branches
                    (let [used-args  (assoc (:used-args frame) rule
                                       (conj (get (:used-args frame) rule []) call-args))
                          branches   (expand-rule rule-clause context used-args)]
                      (recur (concat
                               (for [branch branches]
                                 {:prefix-clauses prefix-clauses
                                  :prefix-context prefix-context
                                  :clauses        (concatv branch next-clauses)
                                  :used-args      used-args
                                  :pending-guards pending-gs})
                               (next stack))
                             rel))))))))
        rel))))

(defn -resolve-clause [context clause]
  (condp looks-like? clause
    '[[*]] ;; predicate [(pred ?a ?b ?c)]
      (filter-by-pred context clause)

    '[[*] _] ;; function [(fn ?a ?b) ?res]
      (bind-by-fn context clause)

    '[*] ;; pattern
      (let [relation (lookup-pattern context clause)]
        (update-in context [:rels] collapse-rels relation))))

(defn resolve-clause [context clause]
  (if (rule? context clause)
    (let [[source rule] (if (source? (first clause))
                          [(first clause) (next clause)]
                          ['$ clause])
          source (get-in context [:sources source])
          rel    (solve-rule (assoc context :sources {'$ source}) rule)]
      (update-in context [:rels] collapse-rels rel))
    (-resolve-clause context clause)))

(defn -q [context clauses]
  (reduce resolve-clause context clauses))

(defn -collect
  ([context symbols]
    (let [rels (:rels context)]
      (-collect [(make-array (count symbols))] rels symbols)))
  ([acc rels symbols]
    (if-let [rel (first rels)]
      (let [keep-attrs (select-keys (:attrs rel) symbols)]
        (if (empty? keep-attrs)
          (recur acc (next rels) symbols)
          (let [copy-map (to-array (map #(get keep-attrs %) symbols))
                len      (count symbols)]
            (recur (for [t1 acc
                         t2 (:tuples rel)]
                     (let [res (aclone t1)]
                       (dotimes [i len]
                         (when-let [idx (aget copy-map i)]
                           (aset res i (aget t2 idx))))
                       res))
                   (next rels)
                   symbols))))
      acc)))

(defn collect [context symbols]
  (->> (-collect context symbols)
       (map vec)
       set))

(defn find-attrs [q]
  (concat
    (map #(if (sequential? %) (last %) %) (:find q))
    (:with q)))

(defn -aggregate [q context tuples]
  (mapv (fn [form fixed-value i]
          (if (sequential? form)
            (let [[f & args] form
                  vals (map #(nth % i) tuples)
                  args (map #(if (symbol? %) (context-resolve-val context %) %)
                             (butlast args))
                  f    (or (built-in-aggregates f)
                           (context-resolve-val context f))]
              (apply f (concat args [vals])))
            fixed-value))
    (:find q)
    (first tuples)
    (range)))

(defn aggregate [q context resultset]
  (let [group-idxs (->> (map #(when-not (sequential? %1) %2) (:find q) (range))
                        (remove nil?))
        group-fn   (fn [tuple]
                     (map #(nth tuple %) group-idxs))
        grouped    (group-by group-fn resultset)]
    (for [[_ tuples] grouped]
      (-aggregate q context tuples))))

(defn parse-query [query]
  (loop [parsed {}, key nil, qs query]
    (if-let [q (first qs)]
      (if (keyword? q)
        (recur parsed q (next qs))
        (recur (update-in parsed [key] (fnil conj []) q) key (next qs)))
      parsed)))

(defn q [q & inputs]
  (let [q         (if (sequential? q) (parse-query q) q)
        find      (find-attrs q)
        ins       (:in q '[$])
        wheres    (:where q)
        context   (-> (Context. [] {} {})
                    (parse-ins ins inputs))
        resultset (-> context
                    (-q wheres)
                    (collect find))]
    (cond->> resultset
      (:with q)
        (mapv #(subvec % 0 (count (:find q))))
      (not-empty (filter sequential? (:find q)))
        (aggregate q context))))
