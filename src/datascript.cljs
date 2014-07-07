(ns datascript
  (:require
    [clojure.set :as set]
    [clojure.walk :as walk]
    [cljs.reader :refer [read-string]]
    [datascript.btset :refer [btset-by slice]])
  (:require-macros [datascript :refer [combine-cmp case-tree]]))

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


;;;;;;;;;; Searching

(defprotocol ISearch
  (-search [data pattern]))

(defn- compare-key [k o1 o2]
  (let [k1 (get o1 k)
        k2 (get o2 k)]
    (if (and (some? k1) (some? k2))
      (let [t1 (type k1)
            t2 (type k2)]
        (if (= t1 t2)
          (compare k1 k2)
          (compare t1 t2)))
      0)))

(defn- cmp-val [o1 o2]
  (if (and (some? o1) (some? o2))
    (let [t1 (type o1)
          t2 (type o2)]
      (if (= t1 t2)
        (compare o1 o2)
        (compare t1 t2)))
    0))

(defn- cmp [o1 o2]
  (if (and o1 o2)
    (compare o1 o2)
    0))

(defn- cmp-datoms-eavt [d1 d2]
  (combine-cmp
    (cmp     (.-e d1) (.-e d2))
    (cmp     (.-a d1) (.-a d2))
    (cmp-val (.-v d1) (.-v d2))
    (cmp     (.-tx d1) (.-tx d2))))

(defn- cmp-datoms-aevt [d1 d2]
  (combine-cmp
    (cmp     (.-a d1) (.-a d2))
    (cmp     (.-e d1) (.-e d2))
    (cmp-val (.-v d1) (.-v d2))
    (cmp     (.-tx d1) (.-tx d2))))

(defn- cmp-datoms-avet [d1 d2]
  (combine-cmp
    (cmp     (.-a d1) (.-a d2))
    (cmp-val (.-v d1) (.-v d2))
    (cmp     (.-e d1) (.-e d2))
    (cmp     (.-tx d1) (.-tx d2))))

(defrecord DB [schema eavt aevt avet max-eid max-tx]
  Object
  (toString [this]
    (pr-str* this))
    
  ISearch
  (-search [_ [e a v tx]]
    (case-tree [e a (some? v) tx] [
      (slice eavt (Datom. e a v tx nil))                 ;; e a v tx
      (slice eavt (Datom. e a v nil nil))                ;; e a v _
      (->> (slice eavt (Datom. e a nil nil nil))         ;; e a _ tx
           (filter #(= tx (.-tx %))))
      (slice eavt (Datom. e a nil nil nil))              ;; e a _ _
      (->> (slice eavt (Datom. e nil nil nil nil))       ;; e _ v tx
           (filter #(and (= v (.-v %)) (= tx (.-tx %)))))
      (->> (slice eavt (Datom. e nil nil nil nil))       ;; e _ v _
           (filter #(= v (.-v %))))
      (->> (slice eavt (Datom. e nil nil nil nil))       ;; e _ _ tx
           (filter #(= tx (.-tx %))))
      (slice eavt (Datom. e nil nil nil nil))            ;; e _ _ _
      (->> (slice avet (Datom. nil a v nil nil))         ;; _ a v tx
           (filter #(= tx (.-tx %))))
      (slice avet (Datom. nil a v nil nil))              ;; _ a v _
      (->> (slice avet (Datom. nil a nil nil nil))       ;; _ a _ tx
           (filter #(= tx (.-tx %))))
      (slice avet (Datom. nil a nil nil nil))            ;; _ a _ _
      (filter #(and (= v (.-v %)) (= tx (.-tx %))) eavt) ;; _ _ v tx
      (filter #(= v (.-v %)) eavt)                       ;; _ _ v _
      (filter #(= tx (.-tx %)) eavt)                     ;; _ _ _ tx
      eavt])))                                           ;; _ _ _ _
  
(defrecord TxReport [db-before db-after tx-data tempids])

(defn multival? [db attr]
  (= (get-in db [:schema attr :db/cardinality]) :db.cardinality/many))

(defn ref? [db attr]
  (= (get-in db [:schema attr :db/valueType]) :db.type/ref))

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


;;;;;;;;;; Transacting

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

(defn- next-eid [db]
  (inc (:max-eid db)))

(defn- advance-max-eid [db eid]
  (cond-> db
    (> eid (:max-eid db))
      (assoc :max-eid eid)))

(defn- allocate-eid
  ([report eid]
     (update-in report [:db-after] advance-max-eid eid))
  ([report e eid]
     (-> report
       (assoc-in [:tempids e] eid)
       (update-in [:db-after] advance-max-eid eid))))

(defn- with-datom [db datom]
  (if (.-added datom)
    (-> db
      (update-in [:eavt] conj datom)
      (update-in [:aevt] conj datom)
      (update-in [:avet] conj datom)
      (advance-max-eid (.-e datom)))
    (let [removing (first (-search db [(.-e datom) (.-a datom) (.-v datom)]))]
      (-> db
        (update-in [:eavt] disj removing)
        (update-in [:aevt] disj removing)
        (update-in [:avet] disj removing)))))

(defn- transact-report [report datom]
  (-> report
      (update-in [:db-after] with-datom datom)
      (update-in [:tx-data] conj datom)))

(defn- explode [db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] (dissoc entity :db/id)
          v      (if (and (sequential? vs)
                          (multival? db a))
                   vs [vs])]
      [:db/add eid a v])))

(defn- transact-add [report [_ e a v]]
  (let [tx      (current-tx report)
        db      (:db-after report)
        datom   (Datom. e a v tx true)]
    (if (multival? db a)
      (if (empty? (-search db [e a v]))
        (transact-report report datom)
        report)
      (if-let [old-datom (first (-search db [e a]))]
        (if (= (.-v old-datom) v)
          report
          (-> report
            (transact-report (Datom. e a (.-v old-datom) tx false))
            (transact-report datom)))
        (transact-report report datom)))))

(defn- transact-retract-datom [report d]
  (let [tx (current-tx report)]
    (transact-report report (Datom. (.-e d) (.-a d) (.-v d) tx false))))

(defn- transact-entities [report [entity & entities :as es]]
  (let [db (:db-after report)]
    (cond
      (nil? entity)
        (-> report
            (update-in [:db-after :max-tx] inc))
     
      (map? entity)
        (if (:db/id entity)
          (recur report (concat (explode db entity) entities))
          (let [eid    (next-eid db)
                entity (assoc entity :db/id eid)]
            (recur (allocate-eid report eid)
                   (concat [entity] entities))))
     
      :else
        (let [[op e a v] entity]
          (cond
            (= op :db.fn/call)
              (let [[_ f & args] entity]
                (recur report (concat (apply f db args) entities)))

            (neg? e)
              (if-let [eid (get-in report [:tempids e])]
                (recur report (concat [[op eid a v]] entities))
                (recur (allocate-eid report e (next-eid db)) es))
           
            (and (ref? db a) (neg? v))
              (if-let [vid (get-in report [:tempids v])]
                (recur report (concat [[op e a vid]] entities))
                (recur (allocate-eid report v (next-eid db)) es))
           
            (= op :db/add)
              (recur (transact-add report entity) entities)

            (= op :db/retract)
              (if-let [old-datom (first (-search db [e a v]))]
                (recur (transact-retract-datom report old-datom) entities)
                (recur report entities))

            (= op :db.fn/retractAttribute)
              (let [datoms (-search db [e a])]
                (recur (reduce transact-retract-datom report datoms) entities))

            (= op :db.fn/retractEntity)
              (let [datoms (-search db [e])]
                (recur (reduce transact-retract-datom report datoms) entities)))))))

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
  (persistent! (reduce #(reduce conj! %1 (f %2)) (transient #{}) coll)))

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
  (when-let [datoms (not-empty (-search db [eid]))]
    (reduce (fn [entity datom]
              (let [a (.-a datom)
                    v (.-v datom)]
                (if (multival? db (.-a datom))
                  (update-in entity [a] (fnil conj []) v)
                  (assoc entity a v))))
            { :db/id eid } datoms)))

(def ^:const tx0 0x20000000)

(defn empty-db [& [schema]]
  (DB. schema
       (btset-by cmp-datoms-eavt) 
       (btset-by cmp-datoms-aevt)
       (btset-by cmp-datoms-avet)
       0
       tx0))

(defn create-conn [& [schema]]
  (atom (empty-db schema)
        :meta { :listeners  (atom {}) }))

(defn transact [db entities]
  (transact-entities (TxReport. db db [] {}) entities))

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

(defn- components->pattern [index cs]
  (case index
    :eavt (Datom. (nth cs 0 nil) (nth cs 1 nil) (nth cs 2 nil) (nth cs 3 nil) nil)
    :aevt (Datom. (nth cs 1 nil) (nth cs 0 nil) (nth cs 2 nil) (nth cs 3 nil) nil)
    :avet (Datom. (nth cs 2 nil) (nth cs 0 nil) (nth cs 1 nil) (nth cs 3 nil) nil)))

(defn datoms [db index & cs]
  (slice (get db index) (components->pattern index cs)))

(defn seek-datoms [db index & cs]
  (slice (get db index) (components->pattern index cs) (Datom. nil nil nil nil nil)))

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(extend-type Datom
  IPrintWithWriter
  (-pr-writer [d w opts]
    (pr-sequential-writer w pr-writer "#datascript/Datom [" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)])))

(defn datom-from-reader [[e a v tx added]]
  (Datom. e a v tx added))

(extend-type DB
  IPrintWithWriter
  (-pr-writer [db w opts]
    (-write w "#datascript/DB {")
    (-write w ":schema ")
    (pr-writer (.-schema db) w opts)
    (-write w ", :datoms ")
    (pr-sequential-writer w
      (fn [d w opts]
        (pr-sequential-writer w pr-writer "[" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d)]))
      "[" " " "]" opts (.-eavt db))
    (-write w "}")))

(defn db-from-reader [{:keys [schema datoms]}]
  (let [datoms (map (fn [[e a v tx]] (Datom. e a v tx true)) datoms)]
    (DB. schema
         (apply btset-by cmp-datoms-eavt datoms)
         (apply btset-by cmp-datoms-aevt datoms)
         (apply btset-by cmp-datoms-avet datoms)
         (reduce max 0 (map :e datoms))
         (reduce max tx0 (map :tx datoms)))))
