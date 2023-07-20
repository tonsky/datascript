
(in-ns 'datascript.core)

(require
  '[clojure.edn :as edn]
  '[clojure.java.io :as io]
  '[datascript.db :as db]
  '[me.tonsky.persistent-sorted-set :as set])

(import
  '[datascript.db Datom]
  '[java.io PushbackReader]
  '[java.util List]
  '[me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet])

(def ^:private root-addr
  #uuid "d5695966-036d-6740-8541-d80034219c28")

(defprotocol IStorage
  (-store [_ addr data])
  (-restore [_ addr]))

(defn file-storage
  ([dir]
   (file-storage dir {}))
  ([dir opts]
   (.mkdirs (io/file dir))
   (let [{:keys [name-fn write-fn read-fn]
          :or   {name-fn  str
                 write-fn (fn [os o]
                            (with-open [wrt (io/writer os)]
                              (binding [*out* wrt]
                                (pr o))))
                 read-fn  (fn [is]
                            (with-open [rdr (PushbackReader. (io/reader is))]
                              (edn/read rdr)))}} opts]
     (reify IStorage
       (-store [_ addr data]
         (with-open [os (io/output-stream (io/file dir (name-fn addr)))]
           (write-fn os data)))
       (-restore [_ addr]
         (with-open [is (io/input-stream (io/file dir (name-fn addr)))]
           (read-fn is)))))))

(deftype PSSStorage [istorage]
  me.tonsky.persistent_sorted_set.IStorage
  (store [_ ^ANode node]
    (let [addr (squuid)
          keys (map (fn [^Datom d] 
                      [(.-e d) (.-a d) (.-v d) (.-tx d)])
                 (.keys node))
          data (cond-> {:level (.level node)
                        :keys  keys}
                 (instance? Branch node)
                 (assoc :addresses (.addresses ^Branch node)))]
      (-store istorage addr data)
      addr))
  (restore [_ addr]
    (let [{:keys [level keys addresses]} (-restore istorage addr)
          keys' ^List (map (fn [[e a v tx]] (db/datom e a v tx)) keys)]
      (if addresses
        (Branch. (int level) keys' ^List addresses)
        (Leaf. keys')))))

(defn validate-storage [^PersistentSortedSet set istorage]
  (when-some [^PSSStorage pss-storage (.-_storage set)]
    (let [istorage' (.-istorage pss-storage)]
      (when-not (identical? istorage' istorage)
        (throw (ex-info "Database is already stored with another IStorage"
                 {:istorage istorage'}))))))

(defn store
  ([istorage db]
   (store istorage db {}))
  ([istorage db opts]
   (validate-storage (:eavt db) istorage)
   (validate-storage (:aevt db) istorage)
   (validate-storage (:avet db) istorage)
   (let [pss-storage (PSSStorage. istorage)
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
         pss-storage (PSSStorage. istorage)]
     (db/restore-db
       {:schema  schema
        :eavt    (set/restore-by db/cmp-datoms-eavt eavt pss-storage)
        :aevt    (set/restore-by db/cmp-datoms-aevt aevt pss-storage)
        :avet    (set/restore-by db/cmp-datoms-avet avet pss-storage)
        :max-eid max-eid
        :max-tx  max-tx}))))
