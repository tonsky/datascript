(ns tonsky.datomicscript)

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

(defn- bind-symbol [sym scope]
  (cond
    (= '_ sym)    nil
    (symbol? sym) (get scope sym nil)
    :else         sym))

(defn- bind-symbols [form scope]
  (map #(bind-symbol % scope) form))

(defn- search-datoms [where scope]
  (let [[source & bound-pattern] (bind-symbols where scope)]
    (search source bound-pattern)))

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

(defn- normalize-where [where]
  (let [source (first where)]
    (if (and (symbol? source)
             (= \$ (-> source name first)))
      where
      (concat ['$] where))))


(def ^:private built-ins { '= =, '== ==, 'not= not=, '!= not=, '< <, '> >, '<= <=, '>= >=, '+ +, '- -, '* *, '/ /, 'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec, 'max max, 'min min,
                           'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'true? true?, 'false? false?, 'nil? nil? })

(defn- call [[f & args] scope]
  (let [bound-args (bind-symbols args scope)
        f          (or (built-ins f) (scope f))]
    (apply f bound-args)))

(defn- q-impl [scope [where & wheres]]
  (if where
    (cond
      ;; predicate [(pred ?a ?b ?c)]
      (and (= 1 (count where))
           (list? (first where)))
        (when (call (first where) scope)
          (q-impl scope wheres))

      ;; assignment [(f ?a) ?b]
      (and (= 2 (count where))
           (list? (first where))
           (symbol? (second where)))
        (let [res (call (first where) scope)
              scope (assoc scope (second where) res)]
          (q-impl scope wheres))
      
      ;; regular data pattern
      :else
        (let [where  (normalize-where where)
              datoms (search-datoms where scope)
              [_ & pattern] where]
          (mapcat #(q-impl (populate-scope scope pattern %) wheres) datoms)))
    [scope]))


(defn- simplify-in-binding [in-form source idx]
  (cond 
    ;; collection binding [?x ...]
    (and (vector? in-form)
         (= 2    (count in-form))
         (= '... (second in-form)))
      (let [sym (symbol (str "$__auto__coll__source" idx))] 
        {:in     sym
         :source (map #(vector %) source)
         :where  [sym (first in-form)]})

    ;; relation binding [[?a ?b]]
    (and (vector? in-form)
         (= 1 (count in-form))
         (vector? (first in-form)))
      (let [sym (symbol (str "$__auto__rel__source" idx))]
        {:in     sym
         :source source
         :where  (vec (concat [sym] (first in-form)))})
   
    ;; tuple binding [?a ?b]
    (vector? in-form)
      (let [sym (symbol (str "$__auto__tuple__source" idx))]
          {:in     sym
           :source [source]
           :where  (vec (concat [sym] in-form))})
   
    ;; regular binding ?x
    (symbol? in-form)
      {:in in-form
       :source source} ))

(defn- simplify-query [query sources]
  (let [simplified (mapv simplify-in-binding
                         (:in query '[$])
                         sources
                         (range))]
    [(assoc query
       :in    (map :in simplified)
       :where (concat (->> simplified (map :where) (remove nil?))
                      (:where query)))
     (map :source simplified)]))

(defn q [query & sources]
  (let [[query sources] (simplify-query query sources)
        scope (zipmap (:in query) sources)]
    (->> (q-impl scope (:where query))
      (map #(mapv % (:find query)))
      (into #{}))))
