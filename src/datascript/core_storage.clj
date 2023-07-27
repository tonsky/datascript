
(in-ns 'datascript.core)

(require
  '[clojure.edn :as edn]
  '[clojure.java.io :as io]
  '[datascript.db :as db]
  '[me.tonsky.persistent-sorted-set :as set])

(import
  '[datascript.db Datom]
  '[java.util List HashSet]
  '[java.io BufferedOutputStream File FileOutputStream OutputStream PushbackReader] 
  '[me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet RefType Settings])

(def ^:private root-addr
  #uuid "d5695966-036d-6740-8541-d80034219c28")

(defprotocol IStorage
  (-store [_ addr+data-seq]
    "Gives you a sequence of `[addr data]` pairs to serialize and store.
     
     `addr`s are java.util.UUID.
     `data`s are clojure-serializable data structure (maps, keywords, lists, ints etc)")
  
  (-restore [_ addr]
    "Read back and deserialize data stored under single `addr`")
  
  (-list-addresses [_]
    "Return seq that lists all addresses currently stored in your storage.
     Will be used during GC to remove keys that are no longer used.")
  
  (-delete [_ addrs-seq]
    "Delete data stored under `addrs` (seq). Will be called during GC"))

; (defprotocol ICache
;   (-compute-if-absent [_ key compute-fn]))

(defn output-stream ^OutputStream [^File file]
  (let [os (FileOutputStream. file)]
    (proxy [BufferedOutputStream] [os]
      (flush [])
      (close []
        (let [this ^BufferedOutputStream this]
          (proxy-super flush)
          (proxy-super close))))))

(defn file-storage
  ([dir]
   (file-storage dir {}))
  ([dir opts]
   (.mkdirs (io/file dir))
   (let [addr->filename-fn (or (:addr->filename-fn opts) str)
         filename->addr-fn (or (:filename->addr-fn opts) #(UUID/fromString %))
         write-fn  (or 
                     (:write-fn opts)
                     (when-some [freeze-fn (:freeze-fn opts)]
                       (fn [os o]
                         (spit os (freeze-fn o))))
                     (fn [os o]
                       (with-open [wrt (io/writer os)]
                         (binding [*out* wrt]
                           (pr o)))))
         read-fn   (or 
                     (:read-fn opts)
                     (when-some [thaw-fn (:thaw-fn opts)]
                       (fn [is]
                         (thaw-fn (slurp is))))
                     (fn [is]
                       (with-open [rdr (PushbackReader. (io/reader is))]
                         (edn/read rdr))))]
     (reify IStorage
       (-store [_ addr+data-seq]
         (doseq [[addr data] addr+data-seq]
           (with-open [os (output-stream (io/file dir (addr->filename-fn addr)))]
             (write-fn os data))))
       
       (-restore [_ addr]
         (with-open [is (io/input-stream (io/file dir (addr->filename-fn addr)))]
           (read-fn is)))
       
       (-list-addresses [_]
         (mapv #(-> ^File %  .getName filename->addr-fn)
           (.listFiles (io/file dir))))
       
       (-delete [_ addrs-seq]
         (doseq [addr addrs-seq]
           (.delete (io/file dir (addr->filename-fn addr)))))))))

(deftype StorageAdapter [istorage ^Settings settings *buffer]
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
      ; (-store istorage addr data)
      (vswap! *buffer conj! [addr data])
      addr))
  (restore [_ addr]
    (let [{:keys [level keys addresses]} (-restore istorage addr)
          keys' ^List (map (fn [[e a v tx]] (db/datom e a v tx)) keys)]
      (if addresses
        (Branch. (int level) keys' ^List addresses settings)
        (Leaf. keys' settings)))))

(defn validate-storage [^PersistentSortedSet set istorage]
  (when-some [^StorageAdapter adapter (.-_storage set)]
    (let [istorage' (.-istorage adapter)]
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
   (let [eavt     ^PersistentSortedSet (:eavt db)
         settings (.-_settings eavt)
         *buffer  (volatile! (transient []))
         adapter  (StorageAdapter. istorage settings *buffer)
         meta     (merge
                    {:schema   (:schema db)
                     :max-eid  (:max-eid db)
                     :max-tx   (:max-tx db)
                     :eavt     (set/store (:eavt db) adapter)
                     :aevt     (set/store (:aevt db) adapter)
                     :avet     (set/store (:avet db) adapter)}
                    (@#'set/settings->map settings))]
     (vswap! *buffer conj! [root-addr meta])
     (-store istorage (persistent! @*buffer)))))

(defn restore
  ([istorage]
   (restore istorage {}))
  ([istorage opts]
   (let [root (-restore istorage root-addr)
         {:keys [schema eavt aevt avet max-eid max-tx]} root
         settings (@#'set/map->settings root)
         adapter (StorageAdapter. istorage settings nil)]
     (db/restore-db
       {:schema  schema
        :eavt    (set/restore-by db/cmp-datoms-eavt eavt adapter root)
        :aevt    (set/restore-by db/cmp-datoms-aevt aevt adapter root)
        :avet    (set/restore-by db/cmp-datoms-avet avet adapter root)
        :max-eid max-eid
        :max-tx  max-tx}))))

(defn addresses
  "Returns all addresses in use by current db. Anything that is not in
   the return set is safe to be deleted"
  [db]
  (let [set      (HashSet.)
        visit-fn #(.add set %)]
    (.add set root-addr)
    (.walkAddresses ^PersistentSortedSet (:eavt db) visit-fn)
    (.walkAddresses ^PersistentSortedSet (:aevt db) visit-fn)
    (.walkAddresses ^PersistentSortedSet (:avet db) visit-fn)
    set))

(defn collect-garbage [istorage db]
  (let [used   (addresses db)
        unused (->> (-list-addresses istorage)
                 (remove #(contains? used %))
                 (vec))]
    (-delete istorage unused)))
