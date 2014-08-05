(ns datascript.query
  (:require
    [clojure.set :as set]
    [datascript :as d :refer [Datom cmp-val]]
    [datascript.btset :refer [btset-by slice]]))

(defrecord Context [rels sources rules])
;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1)) 
                    (set (keys attrs2))))
(defn concatv [& xs]
  (vec (apply concat xs)))

(defn source? [sym]
  (and (symbol? sym)
       (= \$ (first (name sym)))))

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

#_(sum-rel (Relation. '{?a 0 ?b 1} [#js [1 "Ivan"] #js [2 "Oleg"]])
           (Relation. '{?a 0 ?b 1} [#js [3 15] #js [4 22]]))

(defn prod-rel [rel1 rel2]
  (let [attrs1 (keys (:attrs rel1))
        attrs2 (keys (:attrs rel2))
        idxs1  (to-array (map (:attrs rel1) attrs1))
        idxs2  (to-array (map (:attrs rel2) attrs2))]
    (Relation.
      (into {} (map vector (concat attrs1 attrs2) (range)))
      (for [t1 (:tuples rel1)
            t2 (:tuples rel2)]
        (join-tuples t1 idxs1 t2 idxs2)))))
  
#_(prod-rel (Relation. '{?a 0 ?b 1} [#js [1 "Ivan"] #js [2 "Oleg"]])
            (Relation. '{?c 0 ?d 1} [#js [3 15] #js [4 22]]))
#_(prod-rel (Relation. '{?a "e" ?b "v"} [(Datom. 1 :name "Ivan") (Datom. 2 :name "Oleg")])
            (Relation. '{?c 0 ?d 1} [#js [3 15] #js [4 22]]))

(defn in->rel [form value]
  (condp d/looks-like? form
    '[_ ...] ;; collection binding [?x ...]
      (reduce sum-rel 
        (map #(in->rel (first form) %) value))
    '[[*]]   ;; relation binding [[?a ?b]]
      (reduce sum-rel 
        (map #(in->rel (first form) %) value))
    '[*]     ;; tuple binding [?a ?b]
      (reduce prod-rel
        (map #(in->rel %1 %2) form value))
;;     '%       ;; rules ;; TODO
    '_       ;; regular binding ?x
      (Relation. {form 0} [#js [value]])))

#_(in->rel '[[?k [?min ?max]] ...] {:a [1 2] :b [3 4] :c [5 6]})

(defn parse-in [context [in value]]
  (cond
    (source? in)
      (update-in context [:sources] assoc in value)
    (= '% in)
      (assoc context :rules value)
    :else
      (update-in context [:rels] conj (in->rel in value))))

(defn parse-ins [context ins values]
  (reduce parse-in context (map vector ins values)))

#_(parse-ins (Context. [] {} nil)
           '[$ % [?x ...] [?a ?b]]
           [(d/empty-db)
            777
            [1 2 3 4]
            [:a :b]])

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
    (Relation. (into {}
                     (map vector (concat keep-attrs1 keep-attrs2) (range)))
               new-tuples)))

#_(hash-join (Relation. '{?e 0 ?a 1 ?v 2}
                      [#js [1 :name "Ivan"]
                       #js [2 :name "Igor"]
                       #js [3 :name "Petr"]])
           (Relation. '{?e 0 ?a2 1 ?v2 2}
                      [#js [1 :age 15]
                       #js [3 :age 25]]))

(defn lookup-pattern-db [db pattern]
  ;; TODO optimize with bound attrs min/max values here
  (let [search-pattern (mapv #(if (symbol? %) nil %) pattern)
        datoms         (d/-search db search-pattern)
        attr->prop     (->> (map vector pattern ["e" "a" "v" "t"])
                            (filter (fn [[s _]] (and (symbol? s)
                                                     (not= '_ s))))
                            (into {}))]
    (Relation. attr->prop datoms)))

#_(lookup-pattern-db (-> (d/empty-db)
                       (d/with [[:db/add 1 :name "Ivan"]
                                [:db/add 1 :age  15]
                                [:db/add 2 :name "Petr"]
                                [:db/add 2 :age  34]]))
                   ['?e '?a "Ivan"])

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
                        (filter (fn [[s _]] (and (symbol? s)
                                                 (not= '_ s))))
                        (into {}))]
    (Relation. attr->idx (map to-array data)))) ;; FIXME to-array

(defn lookup-pattern [context clause]
  (let [[source-sym pattern] (if (source? (first clause))
                               [(first clause) (next clause)]
                               ['$ clause])
        source   (get (:sources context) source-sym)]
    (cond
      (instance? d/DB source)
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


(defn- rel-get-single-val [rels sym]
  (when-let [rel (first (filter #(contains? (:attrs %) sym) rels))]
    (aget (first (:tuples rel)) ((:attrs rel) sym))))
  
(defn- rel-contains-attrs? [rel attrs]
  (not (empty? (set/intersection (set attrs) (set (keys (:attrs rel)))))))

(defn- rel-prod-by-attrs [context attrs]
  (let [rels       (filter #(rel-contains-attrs? % attrs) (:rels context))
        production (reduce prod-rel rels)]
    [(update-in context [:rels] #(remove (set rels) %)) production]))

(defn -call-fn [rel f args]
  (fn [tuple]
    ;; TODO raise if not all args are bound
    (let [resolved-args (map #(if (symbol? %)
                                (aget tuple (get (:attrs rel) %))
                                %)
                             args)]
      (apply f resolved-args))))

(defn filter-by-pred [context clause]
  (let [[[f & args]] clause
        pred         (or (get d/built-ins f)
                         (rel-get-single-val (:rels context) f))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        tuple-pred   (-call-fn production pred args)
        new-rel      (update-in production [:tuples] #(filter tuple-pred %))]
    (update-in context [:rels] conj new-rel)))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        fun      (or (get d/built-ins f)
                     (rel-get-single-val (:rels context) f))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        tuple-fn (-call-fn production fun args)
        new-rel  (->> (:tuples production)
                   (map #(let [val (tuple-fn %)
                               rel (in->rel out val)]
                             (prod-rel (Relation. (:attrs production) [%]) rel)))
                   (reduce sum-rel))]
    (update-in context [:rels] conj new-rel)))

(defn resolve-clause [context clause]
  (condp d/looks-like? clause
    ;; TODO rules
    
    '[[*]] ;; predicate [(pred ?a ?b ?c)]
      (filter-by-pred context clause)
        
    '[[*] _] ;; function [(fn ?a ?b) ?res]
      (bind-by-fn context clause)

    '[*] ;; pattern
      (let [relation (lookup-pattern context clause)]
        (update-in context [:rels] collapse-rels relation))))

(defn -q [context clauses]
  (reduce resolve-clause context clauses))

(defn -collect
  ([context symbols]
    (let [symbols-map (into {} (map vector symbols (range)))
          rels        (:rels context)
          data        (-collect [(make-array (count symbols))] rels symbols-map)]
      (set (map vec data))))
  ([acc rels symbols-map]
    (if-let [rel (first rels)]
      (let [keep-attrs (vec (intersect-keys (:attrs rel) symbols-map))]
        (if (empty? keep-attrs)
          (recur acc (next rels) symbols-map)
          (let [idxs-map (mapv #(array ((:attrs rel) %) (symbols-map %)) keep-attrs)]
            (recur (for [t1 acc
                         t2 (:tuples rel)]
                     (let [res (.slice t1 0)] ;; clone
                       (doseq [idxs idxs-map
                               :let [from (aget idxs 0)
                                     to   (aget idxs 1)]]
                         (aset res to (aget t2 from)))
                       res))
                   (next rels)
                   symbols-map))))
      acc)))

(defn q [q & inputs]
  (let [q       (if (sequential? q) (d/parse-query q) q)
        find    (:find q)
        ins     (:in q '[$])
        wheres  (:where q)]
    (-> (Context. [] {} nil)
        (parse-ins ins inputs)
        (-q wheres)
        (-collect find))))


#_(q '[:find  ?e ?n ?a
     :in    $ [?n ...]
     :where [?e :name ?n]
            [?e :age ?a]
            [(even? ?a)]]
  (-> (d/empty-db)
      (d/with [ [:db/add 1 :name "Ivan"]
                [:db/add 1 :age  15]
                [:db/add 2 :name "Petr"]
                [:db/add 2 :age  34]
                [:db/add 3 :name "Ivan"]
                [:db/add 3 :age  22]]))
   ["Ivan" "Petr"])

#_(q '[:find  ?e ?n ?e2 ?n2 ?x ?y
     :where [?e :name ?n]
            [?e2 :name ?n2]
            [(< ?e ?e2)]
            [(not= ?n ?n2)]
            [(vector ?n ?n2) [?x ?y]]]
  (-> (d/empty-db)
      (d/with [ [:db/add 1 :name "Ivan"]
                [:db/add 1 :age  15]
                [:db/add 2 :name "Petr"]
                [:db/add 2 :age  34]
                [:db/add 3 :name "Ivan"]
                [:db/add 3 :age  22]])))
