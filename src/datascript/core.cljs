(ns datascript.core
  (:require
    [datascript.btset :as btset])
  (:require-macros
    [datascript :refer [combine-cmp case-tree]]))

(defrecord Datom [e a v tx added]
  Object
  (toString [this]
    (pr-str this)))

(extend-type Datom
  IHash
  (-hash [d] (or (.-__hash d)
                 (set! (.-__hash d)
                       (-> (hash (.-e d))
                           (hash-combine (hash (.-a d)))
                           (hash-combine (hash (.-v d)))))))
  IEquiv
  (-equiv [d o] (and (= (.-e d) (.-e o))
                     (= (.-a d) (.-a o))
                     (= (.-v d) (.-v o))))

  ISeqable
  (-seq [d] (list (.-e d) (.-a d) (.-v d) (.-tx d) (.-added d))))


;;;;;;;;;; Searching

(defprotocol ISearch
  (-search [data pattern]))

(defn cmp-val [o1 o2]
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

(defrecord DB [schema eavt aevt avet max-eid max-tx refs]
  Object
  (toString [this]
    (pr-str* this))

  ISearch
  (-search [_ [e a v tx]]
    (case-tree [e a (some? v) tx] [
      (btset/slice eavt (Datom. e a v tx nil))                 ;; e a v tx
      (btset/slice eavt (Datom. e a v nil nil))                ;; e a v _
      (->> (btset/slice eavt (Datom. e a nil nil nil))         ;; e a _ tx
           (filter #(= tx (.-tx %))))
      (btset/slice eavt (Datom. e a nil nil nil))              ;; e a _ _
      (->> (btset/slice eavt (Datom. e nil nil nil nil))       ;; e _ v tx
           (filter #(and (= v (.-v %)) (= tx (.-tx %)))))
      (->> (btset/slice eavt (Datom. e nil nil nil nil))       ;; e _ v _
           (filter #(= v (.-v %))))
      (->> (btset/slice eavt (Datom. e nil nil nil nil))       ;; e _ _ tx
           (filter #(= tx (.-tx %))))
      (btset/slice eavt (Datom. e nil nil nil nil))            ;; e _ _ _
      (->> (btset/slice avet (Datom. nil a v nil nil))         ;; _ a v tx
           (filter #(= tx (.-tx %))))
      (btset/slice avet (Datom. nil a v nil nil))              ;; _ a v _
      (->> (btset/slice avet (Datom. nil a nil nil nil))       ;; _ a _ tx
           (filter #(= tx (.-tx %))))
      (btset/slice avet (Datom. nil a nil nil nil))            ;; _ a _ _
      (filter #(and (= v (.-v %)) (= tx (.-tx %))) eavt) ;; _ _ v tx
      (filter #(= v (.-v %)) eavt)                       ;; _ _ v _
      (filter #(= tx (.-tx %)) eavt)                     ;; _ _ _ tx
      eavt])))                                           ;; _ _ _ _

(defn- equiv-index [x y]
  (and (= (count x) (count y))
    (loop [xs (seq x)
           ys (seq y)]
      (cond
        (nil? xs) true
        (= (first xs) (first ys)) (recur (next xs) (next ys))
        :else false))))

(extend-type DB
  IHash
  (-hash [this]
    (or (.-__hash this)
        (set! (.-__hash this) (hash-coll (.-eavt this)))))
  IEquiv
  (-equiv [this other]
    (and (instance? DB other)
         (= (.-schema this) (.-schema other))
         (equiv-index (.-eavt this) (.-eavt other)))))


(defrecord TxReport [db-before db-after tx-data tempids])

(defn multival? [db attr]
  (= (get-in db [:schema attr :db/cardinality]) :db.cardinality/many))

(defn ref? [db attr]
  (= (get-in db [:schema attr :db/valueType]) :db.type/ref))

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

(defn reverse-ref [attr]
  (let [name (name attr)]
    (when (= "_" (nth name 0))
      (keyword (namespace attr) (subs name 1)))))

(defn- explode [db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] (dissoc entity :db/id)
          :let   [reverse-a (reverse-ref a)]
          v      (if (and (or (array? vs) (coll? vs))
                          (not (map? vs))
                          (multival? db a))
                   vs [vs])]
      (if reverse-a
        [:db/add v   reverse-a eid]
        [:db/add eid a         v]))))

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

(defn- transact-tx-data [report [entity & entities :as es]]
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

            (= op :db.fn/cas)
              (let [[_ e a ov nv] entity
                    datoms (-search db [e a])]
                (if (multival? db a)
                  (if (some #(= (.-v %) ov) datoms)
                    (recur (transact-add report [:db/add e a nv]) entities)
                    (throw (js/Error. (str ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov))))
                  (let [v (.-v (first datoms))] 
                    (if (= v ov)
                      (recur (transact-add report [:db/add e a nv]) entities)
                      (throw (js/Error. (str ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov)))))))

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
              (let [e-datoms (-search db [e])
                    v-datoms (mapcat (fn [a] (-search db [nil a e])) (.-refs db))]
                (recur (reduce transact-retract-datom report (concat e-datoms v-datoms)) entities)))))))
