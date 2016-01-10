(ns datascript.query
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [raise]]
   [datascript.arrays :as da]
   [datascript.lru]
   [datascript.impl.entity :as de]
   [datascript.parser :as dp #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple Constant
                                                FindColl FindRel FindScalar FindTuple PlainSymbol
                                                RulesVar SrcVar Variable]])]
   [datascript.pull-api :as dpa]
   [datascript.pull-parser :as dpp])
  #?(:clj (:import [datascript.parser BindColl BindIgnore BindScalar BindTuple
                    Constant FindColl FindRel FindScalar FindTuple PlainSymbol
                    RulesVar SrcVar Variable])))

;; ----------------------------------------------------------------------------

(def ^:const lru-cache-size 100)
(declare built-ins)

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
  (into [] cat xs))

(defn- looks-like? [pattern form]
  (cond
    (= '_ pattern)
      true
    (= '[*] pattern)
      (sequential? form)
    (symbol? pattern)
      (= form pattern)
    (sequential? pattern)
      (if (= (last pattern) '*)
        (and (sequential? form)
             (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                     (map vector (butlast pattern) form)))
        (and (sequential? form)
             (= (count form) (count pattern))
             (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                     (map vector pattern form))))
    :else ;; (predicate? pattern)
      (pattern form)))

(defn source? [sym]
  (and (symbol? sym)
       (= \$ (first (name sym)))))

(defn free-var? [sym]
  (and (symbol? sym)
       (= \? (first (name sym)))))

(defn attr? [form]
  (or (keyword? form) (string? form)))

(defn lookup-ref? [form]
  (looks-like? [attr? '_] form))

;; Relation algebra

(defn join-tuples [t1 #?(:cljs idxs1
                         :clj  ^{:tag "[[Ljava.lang.Object;"} idxs1)
                   t2 #?(:cljs idxs2
                         :clj  ^{:tag "[[Ljava.lang.Object;"} idxs2)]
  (let [l1  (alength idxs1)
        l2  (alength idxs2)
        res (da/make-array (+ l1 l2))]
    (dotimes [i l1]
      (aset res i (#?(:cljs aget :clj get) t1 (aget idxs1 i)))) ;; FIXME aget
    (dotimes [i l2]
      (aset res (+ l1 i) (#?(:cljs aget :clj get) t2 (aget idxs2 i)))) ;; FIXME aget
    res))

(defn sum-rel [a b]
  (Relation. (:attrs a) (into (:tuples a) (:tuples b))))

(defn prod-rel
  ([] (Relation. {} [(da/make-array 0)]))
  ([rel1 rel2]
    (let [attrs1 (keys (:attrs rel1))
          attrs2 (keys (:attrs rel2))
          idxs1  (to-array (map (:attrs rel1) attrs1))
          idxs2  (to-array (map (:attrs rel2) attrs2))]
      (Relation.
        (zipmap (concat attrs1 attrs2) (range))
        (persistent!
          (reduce
            (fn [acc t1]
              (reduce (fn [acc t2]
                        (conj! acc (join-tuples t1 idxs1 t2 idxs2)))
                      acc (:tuples rel2)))
            (transient []) (:tuples rel1)))
        ))))

;; built-ins

(defn- -differ? [& xs]
  (let [l (count xs)]
    (not= (take (/ l 2) xs) (drop (/ l 2) xs))))

(defn- -get-else
  [db e a else-val]
  (if-let [datom (first (db/-search db [e a]))]
    (:v datom)
    else-val))

(defn- -get-some
  [db e & as]
  (reduce
   (fn [_ a]
     (when-let [datom (first (db/-search db [e a]))]
       (reduced [(:a datom) (:v datom)])))
   nil
   as))

(defn- -missing?
  [db e a]
  (nil? (get (de/entity db e) a)))

(defn- and-fn [& args]
  (reduce (fn [a b]
            (if b b (reduced b))) true args))
            
(defn- or-fn [& args]
  (reduce (fn [a b]
            (if b (reduced b) b)) nil args))

(def built-ins {
  '= =, '== ==, 'not= not=, '!= not=, '< <, '> >, '<= <=, '>= >=, '+ +, '- -,
  '* *, '/ /, 'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec, 'max max, 'min min,
  'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'compare compare,
  'rand rand, 'rand-int rand-int,
  'true? true?, 'false? false?, 'nil? nil?, 'some? some?, 'not not, 'and and-fn, 'or or-fn,
  'complement complement, 'identical? identical?, 
  'identity identity, 'meta meta, 'name name, 'type type,
  'count count, 'range range, 'not-empty not-empty, 'empty? empty,
  'str str, 'pr-str pr-str, 'print-str print-str, 'println-str println-str, 'prn-str prn-str, 'subs subs,
  're-find re-find, 're-matches re-matches, 're-seq re-seq,
  '-differ? -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?, 'ground identity})
 
(def built-in-aggregates 
 (letfn [(sum [coll] (reduce + 0 coll))
         (avg [coll] (/ (sum coll) (count coll)))
         (median
           [coll]
           (let [terms (sort coll)
                 size (count coll)
                 med (bit-shift-right size 1)]
             (cond-> (nth terms med)
               (even? size)
               (-> (+ (nth terms (dec med)))
                   (/ 2)))))
         (variance
           [coll]
           (let [mean (avg coll)
                 sum  (sum (for [x coll
                                 :let [delta (- x mean)]]
                             (* delta delta)))]
             (/ sum (count coll))))
         (stddev 
           [coll] 
           (#?(:cljs js/Math.sqrt :clj Math/sqrt) (variance coll)))]
   {'avg      avg
    'median   median
    'variance variance
    'stddev   stddev
    'distinct (comp vec distinct)
    'min      (fn
                ([coll] (reduce (fn [acc x]
                                  (if (neg? (compare x acc))
                                    x acc))
                                (first coll) (next coll)))
                ([n coll]
                  (vec
                    (reduce (fn [acc x]
                              (cond
                                (< (count acc) n)
                                  (sort compare (conj acc x))
                                (neg? (compare x (last acc)))
                                  (sort compare (conj (butlast acc) x))
                                :else acc))
                            [] coll))))
    'max      (fn
                ([coll] (reduce (fn [acc x]
                                  (if (pos? (compare x acc))
                                    x acc))
                                (first coll) (next coll)))
                ([n coll]
                  (vec
                    (reduce (fn [acc x]
                              (cond
                                (< (count acc) n)
                                  (sort compare (conj acc x))
                                (pos? (compare x (first acc)))
                                  (sort compare (conj (next acc) x))
                                :else acc))
                            [] coll))))
    'sum      sum
    'rand     (fn
                ([coll] (rand-nth coll))
                ([n coll] (vec (repeatedly n #(rand-nth coll)))))
    'sample   (fn [n coll]
                (vec (take n (shuffle coll))))
    'count    count
    'count-distinct (fn [coll] (count (distinct coll)))}))


;;

(defn parse-rules [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)] ;; for datascript.js interop
    (group-by ffirst rules)))

(defn bindable-to-seq? [x]
  (or (db/seqable? x) (da/array? x)))

(defn empty-rel [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                        (map :symbol))]
    (Relation. (zipmap vars (range)) [])))

(defprotocol IBinding
  (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (prod-rel))
  
  BindScalar
  (in->rel [binding value]
    (Relation. {(get-in binding [:variable :symbol]) 0} [(into-array [value])]))
  
  BindColl
  (in->rel [binding coll]
    (cond
      (not (bindable-to-seq? coll))
        (raise "Cannot bind value " coll " to collection " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      (empty? coll)
        (empty-rel binding)
      :else
        (reduce sum-rel
          (map #(in->rel (:binding binding) %) coll))))
  
  BindTuple
  (in->rel [binding coll]
    (cond
      (not (bindable-to-seq? coll))
        (raise "Cannot bind value " coll " to tuple " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      (< (count coll) (count (:bindings binding)))
        (raise "Not enough elements in a collection " coll " to bind tuple " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      :else
        (reduce prod-rel
          (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
      (update-in context [:sources] assoc (get-in binding [:variable :symbol]) value)
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
      (assoc context :rules (parse-rules value))
    :else
      (update-in context [:rels] conj (in->rel binding value))))

(defn resolve-ins [context bindings values]
  (reduce resolve-in context (zipmap bindings values)))

;;

(def ^:dynamic *lookup-attrs* nil)
(def ^:dynamic *lookup-source* nil)

(defn getter-fn [attrs attr]
  (let [idx (attrs attr)]
    (if (and (not (nil? *lookup-attrs*))
             (contains? *lookup-attrs* attr))
      (fn [tuple]
          (let [eid (#?(:cljs aget :clj get) tuple idx)]
            (if (number? eid) ;; quick path to avoid fn call
              eid
              (db/entid *lookup-source* eid))))
      (fn [tuple]
        (#?(:cljs aget :clj get) tuple idx)))))

(defn tuple-key-fn [getters]
  (if (== (count getters) 1)
    (first getters)
    (let [getters (to-array getters)]
      (fn [tuple]
        (list* #?(:cljs (.map getters #(% tuple))
                  :clj  (da/into-array (map #(% tuple) getters))))))))

(defn hash-attrs [key-fn tuples]
  (loop [tuples     tuples
         hash-table (transient {})]
    (if-let [tuple (first tuples)]
      (let [key (key-fn tuple)]
        (recur (next tuples)
               (assoc! hash-table key (conj (get hash-table key '()) tuple))))
      (persistent! hash-table))))

(defn hash-join [rel1 rel2]
  (let [tuples1       (:tuples rel1)
        tuples2       (:tuples rel2)
        attrs1        (:attrs rel1)
        attrs2        (:attrs rel2)
        common-attrs  (vec (intersect-keys (:attrs rel1) (:attrs rel2)))
        common-gtrs1  (map #(getter-fn attrs1 %) common-attrs)
        common-gtrs2  (map #(getter-fn attrs2 %) common-attrs)
        keep-attrs1   (keys attrs1)
        keep-attrs2   (vec (set/difference (set (keys attrs2)) (set (keys attrs1))))
        keep-idxs1    (to-array (map attrs1 keep-attrs1))
        keep-idxs2    (to-array (map attrs2 keep-attrs2))
        key-fn1       (tuple-key-fn common-gtrs1)
        hash          (hash-attrs key-fn1 tuples1)
        key-fn2       (tuple-key-fn common-gtrs2)
        new-tuples    (->>
                        (reduce (fn [acc tuple2]
                                  (let [key (key-fn2 tuple2)]
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
        datoms         (db/-search db search-pattern)
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

(defn normalize-pattern-clause [clause]
  (if (source? (first clause))
    clause
    (concat ['$] clause)))

(defn lookup-pattern [source pattern]
  (cond
    (satisfies? db/ISearch source)
      (lookup-pattern-db source pattern)
    :else
      (lookup-pattern-coll source pattern)))

(defn collapse-rels [rels new-rel]
  (loop [rels    rels
         new-rel new-rel
         acc     []]
    (if-let [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))

(defn- rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn- context-resolve-val [context sym]
  (when-let [rel (rel-with-attr context sym)]
    (when-let [tuple (first (:tuples rel))]
      (#?(:cljs aget :clj get) tuple ((:attrs rel) sym)))))

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
                                 (#?(:cljs aget :clj get) tuple (get (:attrs rel) %)))
                                %)
                             args)]
      (apply f resolved-args))))

(defn filter-by-pred [context clause]
  (let [[[f & args]] clause
        pred         (or (get built-ins f)
                         (context-resolve-val context f)
                         (when (nil? (rel-with-attr context f))
                           (throw (ex-info (str "Unknown predicate '" f " in " clause)
                                           {:error :query/where, :form clause, :var f}))))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel      (if pred
                       (let [tuple-pred (-call-fn context production pred args)]
                         (update-in production [:tuples] #(filter tuple-pred %)))
                       (assoc production [:tuples] []))]
    (update-in context [:rels] conj new-rel)))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        binding  (dp/parse-binding out)
        fun      (or (get built-ins f)
                     (context-resolve-val context f)
                     (when (nil? (rel-with-attr context f))
                       (throw (ex-info (str "Unknown function '" f " in " clause)
                                       {:error :query/where, :form clause, :var f}))))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel  (if fun
                   (let [tuple-fn (-call-fn context production fun args)
                        rels     (for [tuple (:tuples production)
                                       :let  [val (tuple-fn tuple)]
                                       :when (not (nil? val))]
                                   (prod-rel (Relation. (:attrs production) [tuple])
                                             (in->rel binding val)))]
                     (if (empty? rels)
                       (prod-rel production (empty-rel binding))
                       (reduce sum-rel rels)))
                   (prod-rel (assoc production [:tuples] []) (empty-rel binding)))]
    (update-in context [:rels] collapse-rels new-rel)))

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
                        :prefix-context context
                        :clauses        [clause]
                        :used-args      {}
                        :pending-guards {}})
           rel   (Relation. final-attrs-map [])]
      (if-let [frame (first stack)]
        (let [[clauses [rule-clause & next-clauses]] (split-with #(not (rule? context %)) (:clauses frame))]
          (if (nil? rule-clause)

            ;; no rules -> expand, collect, sum
            (let [context (solve (:prefix-context frame) clauses)
                  tuples  (-collect context final-attrs)
                  new-rel (Relation. final-attrs-map tuples)]
              (recur (next stack) (sum-rel rel new-rel)))

            ;; has rule -> add guards -> check if dead -> expand rule -> push to stack, recur
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

(defn resolve-pattern-lookup-refs [source pattern]
  (if (satisfies? db/IDB source)
    (let [[e a v tx] pattern]
      (->
        [(if (lookup-ref? e) (db/entid-strict source e) e)
         a
         (if (and v (attr? a) (db/ref? source a) (lookup-ref? v)) (db/entid-strict source v) v)
         (if (lookup-ref? tx) (db/entid-strict source tx) tx)]
        (subvec 0 (count pattern))))
    pattern))

(defn dynamic-lookup-attrs [source pattern]
  (let [[e a v tx] pattern]
    (cond-> #{}
      (free-var? e) (conj e)
      (free-var? tx) (conj tx)
      (and
        (free-var? v)
        (not (free-var? a))
        (db/ref? source a)) (conj v))))

(defn -resolve-clause [context clause]
  (condp looks-like? clause
    [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
      (filter-by-pred context clause)

    [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
      (bind-by-fn context clause)

    ['*] ;; pattern
      (let [[source-sym & pattern] (normalize-pattern-clause clause)
            source   (get (:sources context) source-sym)
            pattern  (resolve-pattern-lookup-refs source pattern)
            relation (lookup-pattern source pattern)
            lookup-source? (satisfies? db/IDB source)]
        (binding [*lookup-source* (when lookup-source? source)
                  *lookup-attrs*  (when lookup-source? (dynamic-lookup-attrs source pattern))]
          (update-in context [:rels] collapse-rels relation)))))

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
      (-collect [(da/make-array (count symbols))] rels symbols)))
  ([acc rels symbols]
    (if-let [rel (first rels)]
      (let [keep-attrs (select-keys (:attrs rel) symbols)]
        (if (empty? keep-attrs)
          (recur acc (next rels) symbols)
          (let [copy-map (to-array (map #(get keep-attrs %) symbols))
                len      (count symbols)]
            (recur (for [#?(:cljs t1
                            :clj ^{:tag "[[Ljava.lang.Object;"} t1) acc
                         t2 (:tuples rel)]
                     (let [res (aclone t1)]
                       (dotimes [i len]
                         (when-let [idx (aget copy-map i)]
                           (aset res i (#?(:cljs aget :clj get) t2 idx))))
                       res))
                   (next rels)
                   symbols))))
      acc)))

(defn collect [context symbols]
  (->> (-collect context symbols)
       (map vec)
       set))

(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (get built-in-aggregates (.-symbol var)))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn -aggregate [find-elements context tuples]
  (mapv (fn [element fixed-value i]
          (if (dp/aggregate? element)
            (let [f    (-context-resolve (:fn element) context)
                  args (map #(-context-resolve % context) (butlast (:args element)))
                  vals (map #(nth % i) tuples)]
              (apply f (concat args [vals])))
            fixed-value))
    find-elements
    (first tuples)
    (range)))

(defn- idxs-of [pred coll]
  (->> (map #(when (pred %1) %2) coll (range))
       (remove nil?)))

(defn aggregate [find-elements context resultset]
  (let [group-idxs (idxs-of (complement dp/aggregate?) find-elements)
        group-fn   (fn [tuple]
                     (map #(nth tuple %) group-idxs))
        grouped    (group-by group-fn resultset)]
    (for [[_ tuples] grouped]
      (-aggregate find-elements context tuples))))

(defprotocol IPostProcess
  (-post-process [find tuples]))

(extend-protocol IPostProcess
  FindRel
  (-post-process [_ tuples] tuples)
  FindColl
  (-post-process [_ tuples] (into [] (map first) tuples))
  FindScalar
  (-post-process [_ tuples] (ffirst tuples))
  FindTuple
  (-post-process [_ tuples] (first tuples)))

(defn- pull [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     [(-context-resolve (:source find) context)
                      (dpp/parse-pull
                        (-context-resolve (:pattern find) context))]))]
    (for [tuple resultset]
      (mapv (fn [env el]
              (if env
                (let [[src spec] env]
                  (dpa/pull-spec src spec [el] false))
                el))
            resolved
            tuple))))

(def ^:private query-cache (volatile! (datascript.lru/lru lru-cache-size)))

(defn memoized-parse-query [q]
  (if-let [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))

(defn q [q & inputs]
  (let [parsed-q      (memoized-parse-query q)
        find          (:find parsed-q)
        find-elements (dp/find-elements find)
        find-vars     (dp/find-vars find)
        result-arity  (count find-elements)
        with          (:with parsed-q)
        ;; TODO utilize parser
        all-vars      (concat find-vars (map :symbol with))
        q             (cond-> q
                        (sequential? q) dp/query->map)
        wheres        (:where q)
        context       (-> (Context. [] {} {})
                        (resolve-ins (:in parsed-q) inputs))
        resultset     (-> context
                        (-q wheres)
                        (collect all-vars))]
    (cond->> resultset
      (:with q)
        (mapv #(vec (subvec % 0 result-arity)))
      (some dp/aggregate? find-elements)
        (aggregate find-elements context)
      (some dp/pull? find-elements)
        (pull find-elements context)
      true
        (-post-process find))))
