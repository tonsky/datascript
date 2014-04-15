(ns tonsky.datomicscript)

(defrecord Datom [e a v])
(defrecord DB [schema ea av])

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


(defn- search-by
  
  ([db prop prop-value]
    (let [path (case prop
                  :e [:ea prop-value]
                  :a [:av prop-value])]
      (for [[k datoms] (get-in db path)
            datom datoms]
        datom)))
  
  ([db prop1 prop1-value
       prop2 prop2-value]
    (let [path (case [prop1 prop2]
                 [:e :a] [:ea prop1-value prop2-value]
                 [:a :e] [:ea prop2-value prop1-value]
                 [:a :v] [:av prop1-value prop2-value]
                 [:v :a] [:av prop1-value prop2-value])]
      (get-in db path)))

  ([db e a v]
    (->> (search-by db :e e :a a)
         (filter #(= v (.-v %))))))

(defn- bind-where [sym scope]
  (cond
    (= '_ sym) nil
    (symbol? sym) (get scope sym nil)
    :else sym))

(defn- search-datoms [db where scope]
  (let [bound-where (mapv #(bind-where % scope) where)
        [e a v]     bound-where
        pattern     (mapv #(if % '? '_) bound-where)]
    (case pattern
      ['? '_ '_] (search-by db :e e)
      ['_ '? '_] (search-by db :a a)
      ['? '? '_] (search-by db :e e :a a)
      ['_ '? '?] (search-by db :a a :v v)
      ['? '? '?] (search-by db e a v))))

(defn- bind-datom [scope where datom]
  (->>
    (map #(when (and (symbol? %1)
                (not (contains? scope %1)))
      [%1 %2])
      where
      [(.-e datom) (.-a datom) (.-v datom)])
    (remove nil?)
    (into scope)))

(defn- q-impl [db scope [where & wheres]]
  (if where
    (let [datoms (search-datoms db where scope)]
      (mapcat #(q-impl db (bind-datom scope where %) wheres) datoms))
    [scope]))

(defn q [query db]
  (let [scopes (q-impl db {} (:where query))]
    (reduce
      #(conj %1 (mapv %2 (:find query)))
      #{}
      scopes)))
