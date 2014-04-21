(ns tonsky.datomicscript
  (:require
    [clojure.set :as set]
    [clojure.walk :as walk]))

(defrecord Datom [e a v])

(defprotocol ISearch
  (-search [data pattern]))

(defrecord DB [schema ea av]
  ISearch
  (-search [db [e a v :as pattern]]
    (case [(when e :+) (when a :+) (when v :+)]
      [:+  nil nil]
        (->> (get-in db [:ea e]) vals (apply concat))
      [nil :+  nil]
        (->> (get-in db [:av a]) vals (apply concat))
      [:+  :+  nil]
        (get-in db [:ea e a])
      [nil :+  :+]
        (get-in db [:av a v])
      [:+  :+  :+]
        (->> (get-in db [:ea e a])
             (filter #(= v (.-v %)))))))

(defn- match-tuple [tuple pattern]
  (every? true?
    (map #(or (nil? %2) (= %1 %2)) tuple pattern)))

(defn- search [data pattern]
  (cond
    (satisfies? ISearch data)
      (-search data pattern)
    (satisfies? ISeqable data)
      (filter #(match-tuple % pattern) data)))

(defn create-database [& [schema]]
  (DB. schema (sorted-map) (sorted-map)))

(defn- update-in-sorted [map path f & args]
  (let [map (if (associative? map) map (sorted-map))
        [k & ks] path]
    (if ks
      (assoc map k (apply update-in-sorted (get map k) ks f args))
      (apply update-in map [k] f args))))

(defn- retract-datom [db datom]
  (-> db
    (update-in-sorted [:ea (.-e datom) (.-a datom)] disj datom)
    (update-in-sorted [:av (.-a datom) (.-v datom)] disj datom)))

(defn- add-datom [db datom]
  (-> db
    (update-in-sorted [:ea (.-e datom) (.-a datom)] (fnil conj #{}) datom)
    (update-in-sorted [:av (.-a datom) (.-v datom)] (fnil conj #{}) datom)))

(defn- wipe-attr [db e a]
  (let [datoms (get-in db [:ea e a])]
    (reduce #(retract-datom %1 %2) db datoms)))

(defn- transact-datom [db [op e a v]]
  (let [datom (Datom. e a v)]
    (case op
      :add
        (if (= :many (get-in db [:schema a :cardinality]))
          (add-datom db datom)
          (-> db
            (wipe-attr e a)
            (add-datom datom)))
      :retract
        (retract-datom db datom))))

(defn- explode-entity [e]
  (if (map? e)
    (let [eid (:db/id e)]
      (mapv (fn [[k v]] [:add eid k v]) (dissoc e :db/id)))
    [e]))

(defn transact [db entities]
  (let [datoms (mapcat explode-entity entities)]
    (reduce transact-datom db datoms)))

(defn next-eid [db & [offset]]
  (let [max-eid (or (-> (:ea db) keys last) 0)]
    (+ max-eid (or offset 1))))


;; QUERIES

(defn- parse-where [where]
  (let [source (first where)]
    (if (and (symbol? source)
             (= \$ (-> source name first)))
      [(first where) (next where)]
      ['$ where])))

(defn- bind-symbol [sym scope]
  (cond
    (= '_ sym)    nil
    (symbol? sym) (get scope sym nil)
    :else         sym))

(defn- bind-symbols [form scope]
  (map #(bind-symbol % scope) form))

(defn- search-datoms [source where scope]
  (search (bind-symbol source scope)
          (bind-symbols where scope)))

(defn- datom->tuple [d]
  (cond
    (= (type d) Datom)  [(.-e d) (.-a d) (.-v d)]
    (satisfies? ISeqable d) d))

(defn- populate-scope [scope where datom]
  (->>
    (map #(when (and (symbol? %1)
                (not (contains? scope %1)))
      [%1 %2])
      where
      (datom->tuple datom))
    (remove nil?)
    (into scope)))

(defn- -differ? [& xs]
  (let [l (count xs)]
    (not= (take (/ l 2) xs) (drop (/ l 2) xs))))

(def ^:private built-ins { '= =, '== ==, 'not= not=, '!= not=, '< <, '> >, '<= <=, '>= >=, '+ +, '- -, '* *, '/ /, 'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec, 'max max, 'min min,
                           'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'true? true?, 'false? false?, 'nil? nil?,
                           '-differ? -differ?})

(defn- call [[f & args] scope]
  (let [bound-args (bind-symbols args scope)
        f          (or (built-ins f) (scope f))]
    (apply f bound-args)))

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

(defn collect [f coll]
  (reduce #(set/union %1 (f %2)) #{} coll))

(defn bind-rule-branch [branch call-args context]
  (let [[[rule & local-args] & body] branch
        replacements (zipmap local-args call-args)
        ;; replacing free vars to unique symbols
        seqid        (:__depth context 0)
        bound-body   (walk/postwalk #(if (and (symbol? %)
                                              (= \? (-> % name first)))
                                       (or (replacements %)
                                           (symbol (str (name %) "__auto__" seqid)))
                                       %)
                                     body)]
    ;; recursion breaker
    ;; adding condition that call args cannot take same values as they took in any previous call to this rule
    (concat
      (for [prev-call-args (get context rule)]
        [(concat ['-differ?] call-args prev-call-args)])
      bound-body)))

(defn -q [in+sources wheres scope]
  (cond
    (not-empty in+sources) ;; parsing ins
      (let [[in source] (first in+sources)]
        (condp looks-like? in
          '[_ ...] ;; collection binding [?x ...]
            (collect #(-q (concat [[(first in) %]] (next in+sources)) wheres scope) source)

          '[[*]]   ;; relation binding [[?a ?b]]
            (collect #(-q (concat [[(first in) %]] (next in+sources)) wheres scope) source)

          '[*]     ;; tuple binding [?a ?b]
            (recur (concat
                     (zipmap in source)
                     (next in+sources))
                   wheres
                   scope)
          '%       ;; rules
            (recur (next in+sources)
                   wheres
                   (assoc scope :__rules (group-by ffirst source)))

          '_       ;; regular binding ?x
            (recur (next in+sources)
                   wheres
                   (assoc scope in source))))

    (not-empty wheres) ;; parsing wheres
      (let [where (first wheres)]
        
        ;; rule (rule ?a ?b ?c)
        (if-let [rule-branches (get (:__rules scope) (first where))]
          (let [[rule & call-args] where
                next-scope (-> scope
                             (update-in [:__rules_ctx rule] conj call-args)
                             (update-in [:__rules_ctx :__depth] inc))
                next-wheres (next wheres)]
            (collect
              #(-q nil
                   (concat (bind-rule-branch % call-args (:__rules_ctx scope)) next-wheres)
                   next-scope)
              rule-branches))
          
          (condp looks-like? where
            '[[*]] ;; predicate [(pred ?a ?b ?c)]
              (when (call (first where) scope)
                (recur nil (next wheres) scope))

            '[[*] _] ;; function [(fn ?a ?b) ?res]
              (let [res (call (first where) scope)]
                (recur [[(second where) res]] (next wheres) scope))

            '[*] ;; pattern
              (let [[source where] (parse-where where)
                    found          (search-datoms source where scope)]
                (collect #(-q nil (next wheres) (populate-scope scope where %)) found))
            )))
   
   :else ;; reached bottom
      #{(mapv scope (:__find scope))}
    ))


;; AGGREGATES

(def ^:private built-in-aggregates {
  'distinct (comp vec distinct)
  'min    (fn
            ([coll] (reduce min coll))
            ([n coll]
              (vec
                (reduce (fn [acc x]
                          (cond
                            (< (count acc) n) (sort (conj acc x))
                            (< x (last acc))  (sort (conj (butlast acc) x))
                            :else             acc))
                        [] coll))))
  'max    (fn
            ([coll] (reduce max coll))
            ([n coll]
              (vec
                (reduce (fn [acc x]
                          (cond
                            (< (count acc) n) (sort (conj acc x))
                            (> x (first acc)) (sort (conj (next acc) x))
                            :else             acc))
                        [] coll))))
  'sum    #(reduce + 0 %)
  'rand   (fn
            ([coll] (rand-nth coll))
            ([n coll] (vec (repeatedly n #(rand-nth coll)))))
  'sample (fn [n coll]
            (vec (take n (shuffle coll))))
  'count  count})

(defn- aggr-group-key [find result]
  (mapv (fn [val sym]
          (if (sequential? sym) nil val))
        result
        find))

(defn- -aggregate [find scope results]
  (mapv (fn [sym val i]
          (if (sequential? sym)
            (let [[f & args] sym
                  vals (map #(get % i) results)
                  args (concat
                        (bind-symbols (butlast args) scope)
                        [vals])
                  f    (or (built-in-aggregates f) (scope f))]
              (apply f args))
            val))
        find
        (first results)
        (range)))

(defn- aggregate [query scope results]
  (let [find (concat (:find query) (:with query))]
    (->> results
         (group-by #(aggr-group-key find %))
         (mapv (fn [[_ results]] (-aggregate (:find query) scope results))))))


;; SUMMING UP

(defn q [query & sources]
  (let [ins->sources (zipmap (:in query '[$]) sources)
        find         (concat
                       (map #(if (sequential? %) (last %) %) (:find query))
                       (:with query))
        results      (-q ins->sources (:where query) {:__find find})]
    (cond->> results
      (:with query)
        (mapv #(subvec % 0 (count (:find query))))
      (not-empty (filter sequential? (:find query)))
        (aggregate query ins->sources))))
