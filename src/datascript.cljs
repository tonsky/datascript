(ns datascript
  (:require
    [clojure.set :as set]
    [clojure.walk :as walk]
    [cljs.reader :refer [read-string]]))

(defrecord Datom [e a v tx added]
  Object
  (toString [this]
    (pr-str this)))

(extend-type Datom
  IHash
  (-hash [d] (or (.-__hash d)
                 (set! (.-__hash d) (hash-coll [(.-e d) (.-a d) (.-v d)]))))
  IEquiv
  (-equiv [d o] (and (= (.-e d) (.-e o)) (= (.-a d) (.-a o)) (= (.-v d) (.-v o))))
  ISeqable
  (-seq [d] (list (.-e d) (.-a d) (.-v d) (.-tx d) (.-added d))))

(defprotocol ISearch
  (-search [data pattern]))

(defrecord DB [schema ea av max-eid max-tx]
  ISearch
  (-search [db [e a v tx added :as pattern]]
    (cond->>
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
               (filter #(= v (.-v %)))))
     tx    (filter #(= tx (.-tx %)))
     added (filter #(= added (.-added %))))))

(defrecord TxReport [db-before db-after tx-data tempids])

(defn multival? [db attr]
  (= (get-in db [:schema attr :db/cardinality]) :db.cardinality/many))

(defn- match-tuple [tuple pattern]
  (every? true?
    (map #(or (nil? %2) (= %1 %2)) tuple pattern)))

(defn- search [data pattern]
  (cond
    (satisfies? ISearch data)
      (-search data pattern)
    (or (satisfies? ISeqable data)
        (array? data))
      (filter #(match-tuple % pattern) data)))

(defn- transact-datom [db datom]
  (if (.-added datom)
    (-> db
      (update-in [:ea (.-e datom) (.-a datom)] (fnil conj #{}) datom)
      (update-in [:av (.-a datom) (.-v datom)] (fnil conj #{}) datom))
    (-> db
      (update-in [:ea (.-e datom) (.-a datom)] disj datom)
      (update-in [:av (.-a datom) (.-v datom)] disj datom))))

(defn- -resolve-eid [eid db]
  (- (.-max-eid db) eid))

(defn- resolve-eid [db d]
  (if (neg? (.-e d))
    (update-in d [:e] -resolve-eid db)
    d))

(defn- entity->ops [db entity]
  (cond
    (map? entity)
      (let [eid (:db/id entity)]
        (for [[a vs] (dissoc entity :db/id)
              v      (if (and (sequential? vs)
                              (multival? db a))
                       vs
                       [vs])]
          [:db/add eid a v]))
    (= (first entity) :db.fn/call)
      (let [[_ f & args] entity]
        (mapcat #(entity->ops db %) (apply f db args)))
    :else
      [entity]))

(defn- op->tx-data [db [op e a v]]
  (let [tx (inc (.-max-tx db))]
    (case op
      :db/add
        (if (= :db.cardinality/many (get-in db [:schema a :db/cardinality]))
          (when (empty? (-search db [e a v]))
            [(->Datom e a v tx true)])
          (if-let [old-datom (first (-search db [e a]))]
            (when (not= (.-v old-datom) v)
              [(->Datom e a (.-v old-datom) tx false)
               (->Datom e a v tx true)])
            [(->Datom e a v tx true)]))
      :db/retract
        (when-let [old-datom (first (-search db [e a v]))]
          [(->Datom e a v tx false)])
      :db.fn/retractAttribute
        (let [datoms (-search db [e a])]
          (map #(assoc % :tx tx :added false) datoms))
      :db.fn/retractEntity
        (let [datoms (-search db [e])]
          (map #(assoc % :tx tx :added false) datoms))
      )))

(defn- entity->tx-data [db entity]
  (->> (entity->ops db entity)
       (mapcat #(op->tx-data db %))))

(defn- -with [db tx-data]
  (->
    (reduce transact-datom db tx-data)
    (update-in [:max-tx] inc)
    (assoc      :max-eid (reduce max (.-max-eid db) (map :e tx-data)))))


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

(defn- populate-scope [scope where datom]
  (->>
    (map #(when (and (symbol? %1)
                     (not (contains? scope %1)))
            [%1 %2])
      where
      datom)
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

(defn- collect [f coll]
  (reduce #(set/union %1 (f %2)) #{} coll))

(defn- bind-rule-branch [branch call-args context]
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

(defn- -q [in+sources wheres scope]
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
            (let [rules (if (string? source) (read-string source) source)]
              (recur (next in+sources)
                     wheres
                     (assoc scope :__rules (group-by ffirst rules))))

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

(defn- parse-query [query]
  (loop [parsed {}, key nil, qs query]
    (if-let [q (first qs)]
      (if (keyword? q)
        (recur parsed q (next qs))
        (recur (update-in parsed [key] (fnil conj []) q) key (next qs)))
      parsed)))

;; SUMMING UP

(defn q [query & sources]
  (let [query        (if (sequential? query) (parse-query query) query)
        ins->sources (zipmap (:in query '[$]) sources)
        find         (concat
                       (map #(if (sequential? %) (last %) %) (:find query))
                       (:with query))
        results      (-q ins->sources (:where query) {:__find find})]
    (cond->> results
      (:with query)
        (mapv #(subvec % 0 (count (:find query))))
      (not-empty (filter sequential? (:find query)))
        (aggregate query ins->sources))))

(defn entity [db eid]
  (when-let [attrs (not-empty (get-in db [:ea eid]))]
    (merge { :db/id eid }
           (for [[attr datoms] attrs]
             (if (multival? db attr)
               [attr (map :v datoms)]
               [attr (.-v (first datoms))])))))


(def ^:const tx0 0x20000000)

(defn empty-db [& [schema]]
  (DB. schema {} {} 0 tx0))

(defn create-conn [& [schema]]
  (atom (empty-db schema)
        :meta { :listeners  (atom {}) }))

(defn transact [db entities]
  (let [raw-datoms (mapcat #(entity->tx-data db %) entities)
        datoms     (map #(resolve-eid db %) raw-datoms)
        tempids    (->> raw-datoms
                     (filter #(neg? (.-e %)))
                     (map #(vector (.-e %) (-resolve-eid (.-e %) db)))
                     (into {}))]
    (TxReport. db (-with db datoms) datoms tempids)))

(defn with [db entities]
  (:db-after (transact db entities)))

(defn -transact! [conn entities]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (transact db entities)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact! [conn entities]
  (let [report (-transact! conn entities)]
    (doseq [[_ callback] @(:listeners (meta conn))]
      (callback report))
    report))
           
(defn listen!
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
     (swap! (:listeners (meta conn)) assoc key callback)
     key))

(defn unlisten! [conn key]
  (swap! (:listeners (meta conn)) dissoc key))

