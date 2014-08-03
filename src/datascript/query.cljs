(ns datascript.query
  (:require
    [clojure.set :as set]
    [datascript :as d :refer [Datom cmp-val]]
    [datascript.btset :refer [btset-by slice]]))

(defrecord Context [rels sources])

;; attrs:
;;   { ?e  [0 :e]
;;     ?a  [0 :v]
;;     ?e2 [1 0]
;;     ?a2 [1 1]}
;; tuples:
;;   [ [(Datom. 1 :age 15) [2 22]]
;;     [(Datom. 2 :age 22) [4 33]]
;;     ... ]
(defrecord Relation [attrs tuples])

(defn get-path [o [i k]]
  (if k
    (-> o (aget i) (get k))
    (get o i)))

(defn intersect-keys [m1 m2]
  (set/intersection (set (keys m1)) (set (keys m2))))

(defn rel-depth [rel]
  (-> (:attrs rel) vals first count))

(defn map-vals [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn concatv [& xs]
  (vec (apply concat xs)))

(defn hash-attrs [paths tuples]
  (loop [tuples     tuples
         hash-table (transient {})]
    (if-let [tuple (first tuples)]
      (let [key (mapv #(get-path tuple %) paths)]
        (recur (next tuples)
               (assoc! hash-table key (conj (get hash-table key []) tuple))))
      (persistent! hash-table))))

(defn hash-join [rel1 rel2]
  (.time js/console "hash-join")
  (let [common-attrs  (vec (intersect-keys (:attrs rel1) (:attrs rel2)))
        tuples1       (:tuples rel1)
        tuples2       (:tuples rel2)
        common-paths1 (mapv (:attrs rel1) common-attrs)
        common-paths2 (mapv (:attrs rel2) common-attrs)
        _             (.time js/console "hash-attrs")
        hash          (hash-attrs common-paths1 (:tuples rel1))
        _             (.timeEnd js/console "hash-attrs")
        rel-depth1    (rel-depth rel1)
        rel-depth2    (rel-depth rel2)
        
        _             (.time js/console "new-tuples")
        new-tuples    (->>
                        (reduce (fn [acc tuple2]
                                  (let [key (mapv #(get-path tuple2 %) common-paths2)]
                                    (if-let [tuples1 (get hash key)]
                                      (let [tuple2 (if (> rel-depth2 1) tuple2 #js [tuple2])]
                                        (reduce (fn [acc tuple1]
                                                  (let [tuple1 (if (> rel-depth1 1) tuple1 #js [tuple1])]
                                                    (conj! acc (.concat tuple1 tuple2))))
                                              acc tuples1))
                                      acc)))
                          (transient []) tuples2)
                        (persistent!))
        _             (.timeEnd js/console "new-tuples")
        
        attrs1        (cond->> (:attrs rel1)
                        (== rel-depth1 1)
                          (map-vals (fn [[y]] [0 y])))
        max-x         (->> (vals attrs1)
                           (map first)
                           (reduce max))
        attrs2        (cond->> (:attrs rel2)
                        (== rel-depth2 1)
                          (map-vals (fn [[y]] [0 y]))
                        true
                          (map-vals (fn [[x y]] [(+ x 1 max-x) y])))]
    (.timeEnd js/console "hash-join")
    (Relation. (merge attrs2 attrs1) new-tuples)))


;; (hash-join (Relation. {'?e [0] '?a [1] '?v [2]}
;;                       [[1 :name "Ivan"]
;;                        [2 :name "Igor"]
;;                        [3 :name "Petr"]])
;;            (Relation. {'?e [0] '?age [2]}
;;                       [[1 :age 15]
;;                        [3 :age 25]]))
;; (hash-join (Relation. {'?e [0] '?sex [2]}
;;                       [[1 :sex :m]
;;                        [3 :sex :f]])
;;            (Relation. {'?e [0 0] '?name [0 2] '?age [1 2] }
;;                       [[[1 :name "Ivan"] [1 :age 15]]
;;                        [[2 :name "Igor"] [3 :age 22]]
;;                        [[3 :name "Petr"] [3 :age 25]]]))

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
        attr->path     (->> (map (fn [symbol i] [symbol [i]]) pattern [:e :a :v :t])
                            (filter (fn [[s _]] (and (symbol? s)
                                                     (not= '_ s))))
                            (into {}))]
    (Relation. attr->path datoms)))

;; (build-relation-db (-> (d/empty-db)
;;                        (d/with [[:db/add 1 :name "Ivan"]
;;                                 [:db/add 1 :age  15]
;;                                 [:db/add 2 :name "Petr"]
;;                                 [:db/add 2 :age  34]]))
;;                    ['?e '?a "Ivan"])

(defn build-relation-coll [coll pattern]
  (let [data       (filter #(match-pattern pattern %) coll)
        attr->path (->> (map (fn [symbol i] [symbol [i]]) pattern (range))
                        (filter (fn [[s _]] (and (symbol? s)
                                                 (not= '_ s))))
                        (into {}))]
    (Relation. attr->path data)))

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
  (reduce resolve-clause (Context. [] sources) clauses))

(defn -collect [acc rels symbols]
  (if-let [rel (first rels)]
    (let [paths (->> symbols
                     (map (:attrs rel))
                     (remove nil?))]
      (if (empty? paths)
        (recur acc (next rels) symbols)
        (recur (for [t1 acc
                     t2 (:tuples rel)]
                 (concatv t1 (mapv #(get-path t2 %) paths)))
               (next rels)
               symbols)))
    acc))

#_(-q {'$ [[1 :name "Ivan" 42]
         [1 :age  15 34]
         [2 :name "Petr" 42]
         [2 :age  34 43]]}
    '[[$ ?e :name ?n ?t]
      [$ ?e :age  ?a ?t2]])

(defn search [db]
  (.time js/console "search/-q")
  (let [context (-q {'$ db}
                    '[[$ ?e :name "Ivan"]
                      [$ ?e :age ?a]])
        _ (.timeEnd js/console "search/-q")]
    (-collect [[]] (:rels context) '[?e ?a])))


#_(search (-> (d/empty-db)
            (d/with [ [:db/add 1 :name "Ivan"]
                      [:db/add 1 :age  15]
                      [:db/add 2 :name "Petr"]
                      [:db/add 2 :age  34]])))

;; (defn -search2 [db a1 v1 a2]
;;   (let [rel-e (slice (.-avet db)
;;                      (Datom. nil a1 v1 nil nil))
;;         hash1 (hash-attr #(.-e %) rel-e)
;;         rel-a (slice (.-aevt db)
;;                      (Datom. (:min hash1) a2 nil nil nil)
;;                      (Datom. (:max hash1) a2 nil nil nil))]
;;     (hash-join [#(.-e %)] hash1 rel-a #(.-e %) [#(.-v %)])))

;; (defn search2 [db] (-search2 db :name "Ivan" :age))

;; (defn -search3 [db a1 v1 a2]
;;   (let [rel-e (slice (.-avet db)
;;                      (Datom. nil a1 v1 nil nil))
;;         min   (.-e (first rel-e))
;;         rel-a (slice (.-aevt db)
;;                      (Datom. min a2 nil nil nil)
;;                      (Datom. nil a2 nil nil nil))]
;;     (sort-join rel-e #(.-e %) [#(.-e %)] 
;;                rel-a #(.-e %) [#(.-v %)])))

;; (defn search3 [db] (-search3 db :name "Ivan" :age))


(defn in? [vector el]
  (>= (.indexOf vector el) 0))

(defn bound? [symbol vars]
  (and (symbol? symbol)
       (in? vars symbol)))

(defn unbound? [symbol vars]
  (and (symbol? symbol)
       (not (in? vars symbol))))


;; (defn -q-clause [where vars data]
;;   (let [bound   (filter #(bound? % vars) where)
;;         unbound (filter #(unbound? % vars) where)
;;         bound-vals (into {} (for [var bound]
;;                               [var (build-var var data)]))]
;;     ...

;; (defn -q [ins->sources where vars data])
  

;; (defn q [query & sources]
;;   (-q (zipmap (:in query '[$]) sources)
;;       (:where query)
;;       [] []))

;; (defn aconj [arr x]
;;   (datascript.btset/insert arr (alength arr) #js [x]))



;; (defn get-avet [db a v]
;;   (->> (slice (.-avet db) (Datom. nil a v nil nil))
;;        (mapv #(vector (.-e %)))))

;; (defn get-aevt [db a es]
;;   (let [lookup-hash (hash-attr #(nth % 0) es)
;;         lookup-map  (:hash-table lookup-hash)
;; ;;                     {:min (aget (first es) 0)
;; ;;                      :max (aget (nth es (dec (count es))) 0)}
;;         datoms      (slice (.-aevt db)
;;                       (Datom. (:min lookup-hash) a nil nil nil)
;;                       (Datom. (:max lookup-hash) a nil nil nil))]
;;     (->>
;; ;;       (loop [_res (transient [])
;; ;;              _ds  datoms
;; ;;              _es  es]
;; ;;         (let [_fd (first _ds)
;; ;;               _fe (first _es)]
;; ;;           (if (and _fd _fe)
;; ;;             (condp == (cmp-val (.-e _fd) (aget _fe 0))
;; ;;               0  (recur (conj! _res #js [(aget _fe 0) (.-v _fd)]) (next _ds) (next _es))
;; ;;               -1 (recur _res (next _ds) _es)
;; ;;               1  (recur _res _ds (next _es))
;; ;;               )
;; ;;             _res)))

;;       (reduce (fn [acc d]
;;                 (if-let [datas (get lookup-map (.-e d))]
;;                   (reduce (fn [acc data]
;;                             (conj! acc (conj data (.-v d))))
;;                     acc datas)
;;                   acc))
;;         (transient []) datoms)
;;       (persistent!))))

;; (defn search [db]
;;   (->> (get-avet db :name "Ivan")
;;        (get-aevt db :age)))

