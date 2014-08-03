(ns datascript.query
  (:require
    [clojure.set :as set]
    [datascript :as d :refer [Datom cmp-val]]
    [datascript.btset :refer [btset-by slice]]))

(defrecord Context [rels sources])
;; attrs:
;;    [?e ?v ?e2 ?age]
;; tuples:
;;   [ #js [1 "Ivan" 5 14]
;;     #js [2 "Oleg" 1 55]
;;     ... ]
(defrecord Relation [attrs tuples])

(defn intersect-keys [a1 a2]
  (set/intersection (set a1)) (set a2))

(defn concatv [& xs]
  (vec (apply concat xs)))

(defn index [coll]
  (map vector coll (range)))

(defn index-map [coll]
  (into {} (index coll)))

(defn tuple-key-fn [idxs]
  (if (== (alength idxs) 1)
    (let [idx (first idxs)]
      (fn [tuple]
        (aget tuple idx)))
    (fn [tuple]
      (list* (.map idxs #(aget tuple %))))))

(defn hash-attrs [idxs tuples]
  (let [key-fn (tuple-key-fn idxs)]
    (loop [tuples     tuples
           hash-table (transient {})]
      (if-let [tuple (first tuples)]
        (let [key (key-fn tuple)]
          (recur (next tuples)
                 (assoc! hash-table key (conj (get hash-table key '()) tuple))))
        (persistent! hash-table)))))

(defn join-tuples [t1 t2 idxs2]
  (let [l1  (alength t1)
        l2  (count idxs2)
        res (js/Array. (+ l1 l2))]
    (dotimes [i l1]
      (aset res i (aget t1 i)))
    (dotimes [i l2]
      (aset res (+ l1 i) (aget t2 (nth idxs2 i))))
    res))

(defn hash-join [rel1 rel2]
;;  (.time js/console "hash-join")
  (let [[rel1 rel2]   (if (< (count (:tuples rel1)) (count (:tuples rel2))) [rel1 rel2] [rel2 rel1])
        tuples1       (:tuples rel1)
        tuples2       (:tuples rel2)
        attrs1-map    (index-map (:attrs rel1))
        attrs2-map    (index-map (:attrs rel2))
        common-attrs  (vec (set/intersection (set (keys attrs1-map)) (set (keys attrs2-map))))
        common-idxs1  (to-array (map attrs1-map common-attrs))
        common-idxs2  (to-array (map attrs2-map common-attrs))
        keep-attrs+idx2 (->> attrs2-map
                             (remove (fn [[a i]] (contains? attrs1-map a)))
                             (sort-by second))
        keep-attrs2   (mapv first keep-attrs+idx2)
        keep-idxs2    (mapv second keep-attrs+idx2)
        
;;        _             (.time js/console "hash-attrs")
        hash          (hash-attrs common-idxs1 (:tuples rel1))
;;        _             (.timeEnd js/console "hash-attrs")
        
;;        _             (.time js/console "new-tuples")
;;        _             (.log js/console (str "t1: " (count tuples1) ", hash: " (count hash) ", t2: "(count tuples2)))
        key-fn        (tuple-key-fn common-idxs2)
        new-tuples    (->>
                        (reduce (fn [acc tuple2]
                                  (let [key (key-fn tuple2)]
                                    (if-let [tuples1 (get hash key)]
                                      (reduce (fn [acc tuple1]
                                                (conj! acc (join-tuples tuple1 tuple2 keep-idxs2)))
                                              acc tuples1)
                                      acc)))
                          (transient []) tuples2)
                        (persistent!))
;;        _             (.timeEnd js/console "new-tuples")
        ]
;;    (.timeEnd js/console "hash-join")
    (Relation. (concatv (:attrs rel1) keep-attrs2)
               new-tuples)))


(hash-join (Relation. '[?e ?a ?v]
                      [#js [1 :name "Ivan"]
                       #js [2 :name "Igor"]
                       #js [3 :name "Petr"]])
           (Relation. '[?e ?a2 ?v2]
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
        attr+prop      (->> (map vector pattern ["e" "a" "v" "t"])
                            (filter (fn [[s _]] (and (symbol? s)
                                                     (not= '_ s)))))
        props          (to-array (map second attr+prop))]
    (Relation. (mapv first attr+prop)
               (mapv (fn [el] (.map props #(aget el %))) datoms))))

#_(build-relation-db (-> (d/empty-db)
                       (d/with [[:db/add 1 :name "Ivan"]
                                [:db/add 1 :age  15]
                                [:db/add 2 :name "Petr"]
                                [:db/add 2 :age  34]]))
                   ['?e '?a "Ivan"])

(defn build-relation-coll [coll pattern]
  (let [data       (filter #(match-pattern pattern %) coll)
        attr+idx   (->> (map vector pattern (range))
                        (filter (fn [[s _]] (and (symbol? s)
                                                 (not= '_ s)))))
        idxs       (to-array (map second attr+idx))]
    (Relation. (mapv first attr+idx)
               (mapv (fn [el] (.map idxs #(nth el %))) data))))

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

(defn -collect [acc rels symbols]
;;  (.time js/console "search/-collect")
  (if-let [rel (first rels)]
    (let [attr->idx (index-map (:attrs rel))
          idxs (->> symbols
                    (map attr->idx)
                    (remove nil?)
                    vec)]
      (if (empty? idxs)
        (recur acc (next rels) symbols)
        (recur (for [t1 acc
                     t2 (:tuples rel)]
                 (join-tuples t1 t2 idxs))
               (next rels)
               symbols)))
    (do
;;      (.timeEnd js/console "search/-collect")
      (set (map vec acc)))))

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
        res (-collect [#js[]] (:rels context) '[?e ?a])]
;;    (.timeEnd js/console "search")  
    res))


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

