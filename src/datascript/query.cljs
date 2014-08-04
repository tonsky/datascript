(ns datascript.query
  (:require
    [clojure.set :as set]
    [datascript :as d :refer [Datom cmp-val]]
    [datascript.btset :refer [btset-by slice]]))

(defrecord Context [rels sources])
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

(defn join-tuples [t1 idxs1 t2 idxs2]
  (let [l1  (alength idxs1)
        l2  (alength idxs2)
        res (js/Array. (+ l1 l2))]
    (dotimes [i l1]
      (aset res i (aget t1 (aget idxs1 i)))) ;; FIXME aget
    (dotimes [i l2]
      (aset res (+ l1 i) (aget t2 (aget idxs2 i)))) ;; FIXME aget
    res))

(defn hash-join [rel1 rel2]
;;  (.time js/console "hash-join")
  (let [;; [rel1 rel2]   (if (< (count (:tuples rel1)) (count (:tuples rel2))) [rel1 rel2] [rel2 rel1])
        tuples1       (:tuples rel1)
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
        
;;        _             (.time js/console "hash-attrs")
        hash          (hash-attrs common-idxs1 tuples1)
;;        _             (.timeEnd js/console "hash-attrs")
        
;;        _             (.time js/console "new-tuples")
;;        _             (.log js/console (str "t1: " (count tuples1) ", hash: " (count hash) ", t2: "(count tuples2)))
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
                        (persistent!))
;;        _             (.timeEnd js/console "new-tuples")
        ]
;;    (.timeEnd js/console "hash-join")
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

(defn match-pattern [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (symbol? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn build-relation-db [db pattern]
  (let [search-pattern (mapv #(if (symbol? %) nil %) pattern)
        datoms         (d/-search db search-pattern)
        attr->prop     (->> (map vector pattern ["e" "a" "v" "t"])
                            (filter (fn [[s _]] (and (symbol? s)
                                                     (not= '_ s))))
                            (into {}))]
    (Relation. attr->prop (vec datoms))))

#_(build-relation-db (-> (d/empty-db)
                       (d/with [[:db/add 1 :name "Ivan"]
                                [:db/add 1 :age  15]
                                [:db/add 2 :name "Petr"]
                                [:db/add 2 :age  34]]))
                   ['?e '?a "Ivan"])

(defn build-relation-coll [coll pattern]
  (let [data       (filter #(match-pattern pattern %) coll)
        attr->idx  (->> (map vector pattern (range))
                        (filter (fn [[s _]] (and (symbol? s)
                                                 (not= '_ s))))
                        (into {}))]
    (Relation. attr->idx (map to-array data)))) ;; FIXME to-array

(defn build-relation [source pattern]
  (cond
    (instance? d/DB source)
      (build-relation-db source pattern)
    :else
      (build-relation-coll source pattern)))

(defn collapse-rels [rels new-rel]
  (loop [rels    rels
         new-rel new-rel
         acc     []]
    (if-let [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))

(defn resolve-clause [context clause]
  (let [[source-symbol & pattern] clause
        source   (get (:sources context) source-symbol)
        relation (build-relation source pattern)]
    (update-in context [:rels] collapse-rels relation)))

(defn -q [sources clauses]
;;  (.time js/console "search/-q")
  (let [res (reduce resolve-clause (Context. [] sources) clauses)]
;;    (.timeEnd js/console "search/-q")
    res))

(defn -collect
  ([context symbols]
    ;;  (.time js/console "search/-collect")
    (let [symbols-map (into {} (map vector symbols (range)))
          rels        (:rels context)
          data        (-collect [(make-array (count symbols))] rels symbols-map)
          res         (set (map vec data))]
      ;;      (.timeEnd js/console "search/-collect")
      res))
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

#_(-q {'$ [[1 :name "Ivan" 42]
         [1 :age  15 34]
         [2 :name "Petr" 42]
         [2 :age  34 43]]}
    '[[$ ?e :name ?n ?t]
      [$ ?e :age  ?a ?t2]])

(defn search [db]
;;  (.time js/console "search")
  (let [context (-q {'$ db}
                    '[[$ ?e :name "Ivan"]
                      [$ ?e :age ?a]])
        res (-collect context '[?e ?a])]
;;    (.timeEnd js/console "search")  
    res))


#_(search (-> (d/empty-db)
            (d/with [ [:db/add 1 :name "Ivan"]
                      [:db/add 1 :age  15]
                      [:db/add 2 :name "Petr"]
                      [:db/add 2 :age  34]
                      [:db/add 3 :name "Ivan"]
                      [:db/add 3 :age  22]])))

(defn in? [vector el]
  (>= (.indexOf vector el) 0))

(defn bound? [symbol vars]
  (and (symbol? symbol)
       (in? vars symbol)))

(defn unbound? [symbol vars]
  (and (symbol? symbol)
       (not (in? vars symbol))))


