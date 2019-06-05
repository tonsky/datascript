(ns ^:no-doc datascript.query
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [raise]]
   [me.tonsky.persistent-sorted-set.arrays :as da]
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

(declare -collect -resolve-clause resolve-clause)

;; Records

(defrecord Context [rels sources rules])

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])


;; Utilities

(defn single [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1))
                    (set (keys attrs2))))

(defn concatv [& xs]
  (into [] cat xs))

(defn zip
  ([a b] (mapv vector a b))
  ([a b & rest] (apply mapv vector a b rest)))

(defn same-keys? [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? b %) (keys a))))

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
      (aset res i (#?(:cljs da/aget :clj get) t1 (aget idxs1 i)))) ;; FIXME aget
    (dotimes [i l2]
      (aset res (+ l1 i) (#?(:cljs da/aget :clj get) t2 (aget idxs2 i)))) ;; FIXME aget
    res))

(defn sum-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b]
    (cond
      (= attrs-a attrs-b)
      (Relation. attrs-a (into (vec tuples-a) tuples-b))

      (not (same-keys? attrs-a attrs-b))
      (raise "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
             {:error :query/where})

      (every? number? (vals attrs-a)) ;; can’t conj into BTSetIter
      (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                              [idx-b (attrs-a sym)]))
            tlen    (->> (vals attrs-a) (reduce max) (inc)) 
            tuples' (persistent!
                      (reduce
                        (fn [acc tuple-b]
                          (let [tuple' (da/make-array tlen)]
                            (doseq [[idx-b idx-a] idxb->idxa]
                              (aset tuple' idx-a (#?(:cljs da/aget :clj get) tuple-b idx-b)))
                            (conj! acc tuple')))
                        (transient (vec tuples-a))
                        tuples-b))]
        (Relation. attrs-a tuples'))

      :else
      (let [all-attrs (zipmap (keys (merge attrs-a attrs-b)) (range))]
        (-> (Relation. all-attrs [])
            (sum-rel a)
            (sum-rel b))))))

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
  (when (nil? else-val)
    (raise "get-else: nil default value is not supported" {:error :query/where}))
  (if-some [datom (first (db/-search db [e a]))]
    (:v datom)
    else-val))

(defn- -get-some
  [db e & as]
  (reduce
   (fn [_ a]
     (when-some [datom (first (db/-search db [e a]))]
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
  'identity identity, 'meta meta, 'name name, 'namespace namespace, 'type type,
  'vector vector, 'list list, 'set set, 'hash-map hash-map, 'array-map array-map,
  'count count, 'range range, 'not-empty not-empty, 'empty? empty, 'contains? contains?,
  'str str, 'pr-str pr-str, 'print-str print-str, 'println-str println-str, 'prn-str prn-str, 'subs subs,
  're-find re-find, 're-matches re-matches, 're-seq re-seq, 're-pattern re-pattern,
  '-differ? -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?, 'ground identity,
  'clojure.string/blank? str/blank?, 'clojure.string/includes? str/includes?,
  'clojure.string/starts-with? str/starts-with?, 'clojure.string/ends-with? str/ends-with?
})

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
    'distinct set
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
      (not (db/seqable? coll))
        (raise "Cannot bind value " coll " to collection " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      (empty? coll)
        (empty-rel binding)
      :else
        (->> coll
          (map #(in->rel (:binding binding) %))
          (reduce sum-rel))))
  
  BindTuple
  (in->rel [binding coll]
    (cond
      (not (db/seqable? coll))
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
      (update context :sources assoc (get-in binding [:variable :symbol]) value)
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
      (assoc context :rules (parse-rules value))
    :else
      (update context :rels conj (in->rel binding value))))

(defn resolve-ins [context bindings values]
  (reduce resolve-in context (zipmap bindings values)))

;;

(def ^{:dynamic true
       :doc "List of symbols in current pattern that might potentiall be resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc "Default pattern source. Lookup refs, patterns, rules will be resolved with it"}
  *implicit-source* nil)

(defn getter-fn [attrs attr]
  (let [idx (attrs attr)]
    (if (contains? *lookup-attrs* attr)
      (fn [tuple]
        (let [eid (#?(:cljs da/aget :clj get) tuple idx)]
          (cond
            (number? eid)     eid ;; quick path to avoid fn call
            (sequential? eid) (db/entid *implicit-source* eid)
            (da/array? eid)   (db/entid *implicit-source* eid)
            :else             eid)))
      (fn [tuple]
        (#?(:cljs da/aget :clj get) tuple idx)))))

(defn tuple-key-fn [getters]
  (if (== (count getters) 1)
    (first getters)
    (let [getters (to-array getters)]
      (fn [tuple]
        (list* #?(:cljs (.map getters #(% tuple))
                  :clj  (to-array (map #(% tuple) getters))))))))

(defn hash-attrs [key-fn tuples]
  (loop [tuples     tuples
         hash-table (transient {})]
    (if-some [tuple (first tuples)]
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
                                    (if-some [tuples1 (get hash key)]
                                      (reduce (fn [acc tuple1]
                                                (conj! acc (join-tuples tuple1 keep-idxs1 tuple2 keep-idxs2)))
                                              acc tuples1)
                                      acc)))
                          (transient []) tuples2)
                        (persistent!))]
    (Relation. (zipmap (concat keep-attrs1 keep-attrs2) (range))
               new-tuples)))

(defn subtract-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b
        attrs     (intersect-keys attrs-a attrs-b)
        getters-b (map #(getter-fn attrs-b %) attrs)
        key-fn-b  (tuple-key-fn getters-b)
        hash      (hash-attrs key-fn-b tuples-b)
        getters-a (map #(getter-fn attrs-a %) attrs)
        key-fn-a  (tuple-key-fn getters-a)]
    (assoc a
      :tuples (filterv #(nil? (hash (key-fn-a %))) tuples-a))))

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
    (Relation. attr->idx (mapv to-array data)))) ;; FIXME to-array

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
    (if-some [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))

(defn- rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn- context-resolve-val [context sym]
  (when-some [rel (rel-with-attr context sym)]
    (when-some [tuple (first (:tuples rel))]
      (#?(:cljs da/aget :clj get) tuple ((:attrs rel) sym)))))

(defn- rel-contains-attrs? [rel attrs]
  (some #(contains? (:attrs rel) %) attrs))

(defn- rel-prod-by-attrs [context attrs]
  (let [rels       (filter #(rel-contains-attrs? % attrs) (:rels context))
        production (reduce prod-rel rels)]
    [(update context :rels #(remove (set rels) %)) production]))

(defn -call-fn [context rel f args]
  (let [sources     (:sources context)
        attrs       (:attrs rel)
        len         (count args)
        static-args (da/make-array len)
        tuples-args (da/make-array len)]
    (dotimes [i len]
      (let [arg (nth args i)]
        (if (symbol? arg) 
          (if-some [source (get sources arg)]
            (da/aset static-args i source)
            (da/aset tuples-args i (get attrs arg)))
          (da/aset static-args i arg))))
    ;; CLJS `apply` + `vector` will hold onto mutable array of arguments directly
    ;; https://github.com/tonsky/datascript/issues/262
    (if #?(:clj  false
           :cljs (identical? f vector))
      (fn [tuple]
        ;; TODO raise if not all args are bound
        (let [args (da/aclone static-args)]
          (dotimes [i len]
            (when-some [tuple-idx (aget tuples-args i)]
              (let [v (#?(:cljs da/aget :clj get) tuple tuple-idx)]
                (da/aset args i v))))
          (apply f args)))
      (fn [tuple]
        ;; TODO raise if not all args are bound
        (dotimes [i len]
          (when-some [tuple-idx (aget tuples-args i)]
            (let [v (#?(:cljs da/aget :clj get) tuple tuple-idx)]
              (da/aset static-args i v))))
        (apply f static-args)))))

(defn- resolve-sym [sym]
  #?(:cljs nil
     :clj (when (namespace sym)
            (when-some [v (resolve sym)] @v))))

(defn filter-by-pred [context clause]
  (let [[[f & args]] clause
        pred         (or (get built-ins f)
                         (context-resolve-val context f)
                         (resolve-sym f)
                         (when (nil? (rel-with-attr context f))
                           (raise "Unknown predicate '" f " in " clause
                                  {:error :query/where, :form clause, :var f})))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel      (if pred
                       (let [tuple-pred (-call-fn context production pred args)]
                         (update production :tuples #(filter tuple-pred %)))
                       (assoc production :tuples []))]
    (update context :rels conj new-rel)))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        binding  (dp/parse-binding out)
        fun      (or (get built-ins f)
                     (context-resolve-val context f)
                     (resolve-sym f)
                     (when (nil? (rel-with-attr context f))
                       (raise "Unknown function '" f " in " clause
                              {:error :query/where, :form clause, :var f})))
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
                   (prod-rel (assoc production :tuples []) (empty-rel binding)))]
    (update context :rels collapse-rels new-rel)))

;;; RULES

(defn rule? [context clause]
  (and (sequential? clause)
       (contains? (:rules context)
                  (if (source? (first clause))
                    (second clause)
                    (first clause)))))

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
          (db/some-of
            (replacements %)
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

(defn collect-vars [clause]
  (set (walk-collect clause free-var?)))

(defn split-guards [clauses guards]
  (let [bound-vars (collect-vars clauses)
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
      (if-some [frame (first stack)]
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
        [(if (or (lookup-ref? e) (attr? e)) (db/entid-strict source e) e)
         a
         (if (and v (attr? a) (db/ref? source a) (or (lookup-ref? v) (attr? v))) (db/entid-strict source v) v)
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

(defn limit-rel [rel vars]
  (when-some [attrs' (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs')))

(defn limit-context [context vars]
  (assoc context
    :rels (->> (:rels context)
               (keep #(limit-rel % vars)))))

(defn check-bound [context vars form]
  (let [bound (into #{} (mapcat #(keys (:attrs %)) (:rels context)))]
    (when-not (set/subset? vars bound)
      (let [missing (set/difference (set vars) bound)]
        (raise "Insufficient bindings: " missing " not bound in " form
               {:error :query/where
                :form  form
                :vars  missing})))))

(defn -resolve-clause
  ([context clause]
    (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     (filter-by-pred context clause)
     
     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (bind-by-fn context clause)
     
     [source? '*] ;; source + anything
     (let [[source-sym & rest] clause]
       (binding [*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))
     
     '[or *] ;; (or ...)
     (let [[_ & branches] clause
           contexts (map #(resolve-clause context %) branches)
           rels     (map #(reduce hash-join (:rels %)) contexts)]
       (assoc (first contexts) :rels [(reduce sum-rel rels)]))
     
     '[or-join [[*] *] *] ;; (or-join [[req-vars] vars] ...)
     (let [[_ [req-vars & vars] & branches] clause]
       (check-bound context req-vars orig-clause)
       (recur context (list* 'or-join (concat req-vars vars) branches) clause))
     
     '[or-join [*] *] ;; (or-join [vars] ...)
     ;; TODO required vars
     (let [[_ vars & branches] clause
           vars         (set vars)
           join-context (limit-context context vars)
           contexts     (map #(-> join-context (resolve-clause %) (limit-context vars)) branches)
           rels         (map #(reduce hash-join (:rels %)) contexts)
           sum-rel      (reduce sum-rel rels)]
       (update context :rels collapse-rels sum-rel))
     
     '[and *] ;; (and ...)
     (let [[_ & clauses] clause]
       (reduce resolve-clause context clauses))
     
     '[not *] ;; (not ...)
     (let [[_ & clauses] clause
           bound-vars       (set (mapcat #(keys (:attrs %)) (:rels context)))
           negation-vars    (collect-vars clauses)
           _                (when (empty? (set/intersection bound-vars negation-vars))
                              (raise "Insufficient bindings: none of " negation-vars " is bound in " orig-clause
                                {:error :query/where
                                 :form  orig-clause}))
           context'         (assoc context :rels [(reduce hash-join (:rels context))])
           negation-context (reduce resolve-clause context' clauses)
           negation         (subtract-rel
                              (single (:rels context'))
                              (reduce hash-join (:rels negation-context)))]
       (assoc context' :rels [negation]))
     
     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause
           _                (check-bound context vars orig-clause)
           context'         (assoc context :rels [(reduce hash-join (:rels context))])
           join-context     (limit-context context' vars)
           negation-context (-> (reduce resolve-clause join-context clauses)
                                (limit-context vars))
           negation         (subtract-rel
                              (single (:rels context'))
                              (reduce hash-join (:rels negation-context)))]
       (assoc context' :rels [negation]))
     
     '[*] ;; pattern
     (let [source   *implicit-source*
           pattern  (resolve-pattern-lookup-refs source clause)
           relation (lookup-pattern source pattern)]
       (binding [*lookup-attrs* (if (satisfies? db/IDB source)
                                  (dynamic-lookup-attrs source pattern)
                                  *lookup-attrs*)]
         (update context :rels collapse-rels relation))))))

(defn resolve-clause [context clause]
  (if (rule? context clause)
    (if (source? (first clause))
      (binding [*implicit-source* (get (:sources context) (first clause))]
        (resolve-clause context (next clause)))
      (update context :rels collapse-rels (solve-rule context clause)))
    (-resolve-clause context clause)))

(defn -q [context clauses]
  (binding [*implicit-source* (get (:sources context) '$)]
    (reduce resolve-clause context clauses)))

(defn -collect
  ([context symbols]
    (let [rels (:rels context)]
      (-collect [(da/make-array (count symbols))] rels symbols)))
  ([acc rels symbols]
    (if-some [rel (first rels)]
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
                         (when-some [idx (aget copy-map i)]
                           (aset res i (#?(:cljs da/aget :clj get) t2 idx))))
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
    (or (get built-in-aggregates (.-symbol var))
        (resolve-sym (.-symbol var))))
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
  (if-some [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))

(defn q [q & inputs]
  (let [parsed-q      (memoized-parse-query q)
        find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        find-vars     (dp/find-vars find)
        result-arity  (count find-elements)
        with          (:qwith parsed-q)
        ;; TODO utilize parser
        all-vars      (concat find-vars (map :symbol with))
        q             (cond-> q
                        (sequential? q) dp/query->map)
        wheres        (:where q)
        context       (-> (Context. [] {} {})
                        (resolve-ins (:qin parsed-q) inputs))
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
