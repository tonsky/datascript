
(in-ns 'datascript.core)

(require
  '[datascript.db :as db]
  '[me.tonsky.persistent-sorted-set :as set])

(import
  '[java.util List]
  '[me.tonsky.persistent_sorted_set ANode Branch Leaf])

(def ^:private root-addr
  #uuid "d5695966-036d-6740-8541-d80034219c28")

(defn- gen-addr []
  (random-uuid))

(defprotocol IStorage
  (-store [_ addr data])
  (-restore [_ addr]))

(defn- pss-storage [istorage]
  (reify me.tonsky.persistent_sorted_set.IStorage
    (store [_ ^ANode node]
      (let [addr (gen-addr)
            data (cond-> {:level (.level node)
                          :keys  (.keys node)}
                   (instance? Branch node)
                   (assoc :addresses (.addresses ^Branch node)))]
        (-store istorage addr data)
        addr))
    (restore [_ addr]
      (let [{:keys [level keys addresses]} (-restore istorage addr)]
        (if addresses
          (Branch. (int level) ^List keys ^List addresses)
          (Leaf. keys))))))

(defn store
  ([istorage db]
   (store istorage db {}))
  ([istorage db opts]
   (let [pss-storage (pss-storage istorage)
         meta {:schema  (:schema db)
               :max-eid (:max-eid db)
               :max-tx  (:max-tx db)
               :eavt    (set/store (:eavt db) pss-storage)
               :aevt    (set/store (:aevt db) pss-storage)
               :avet    (set/store (:avet db) pss-storage)}]
     (-store istorage root-addr meta))))

(defn restore
  ([istorage]
   (restore istorage {}))
  ([istorage opts]
   (let [{:keys [schema eavt aevt avet max-eid max-tx]} (-restore istorage root-addr)
         pss-storage (pss-storage istorage)]
     (db/restore-db
       {:schema  schema
        :eavt    (set/restore-by db/cmp-datoms-eavt eavt pss-storage)
        :aevt    (set/restore-by db/cmp-datoms-aevt aevt pss-storage)
        :avet    (set/restore-by db/cmp-datoms-avet avet pss-storage)
        :max-eid max-eid
        :max-tx  max-tx}))))
