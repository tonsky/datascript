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

;; datascriptstorageroot23718
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

(def ^:private ^:dynamic *store-buffer*)

(defrecord StorageAdapter [storage ^Settings settings]
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
      (vswap! *store-buffer* conj! [addr data])
      addr))
  (restore [_ addr]
    (let [{:keys [level keys addresses]} (-restore storage addr)
          keys' ^List (map (fn [[e a v tx]] (db/datom e a v tx)) keys)]
      (if addresses
        (Branch. (int level) keys' ^List addresses settings)
        (Leaf. keys' settings)))))

(defn- make-adapter [storage opts]
  (let [settings (@#'set/map->settings opts)]
    (->StorageAdapter storage settings)))

(defn- adapter ^StorageAdapter [db]
  (.-_storage ^PersistentSortedSet (:eavt db)))

(defn storage
  "Returns IStorage used by current DB instance"
  [db]
  (when-some [adapter (adapter db)]
    (:storage adapter)))

(defn- store-impl! [db adapter opts]
  (binding [*store-buffer* (volatile! (transient []))]
    (let [eavt     ^PersistentSortedSet (:eavt db)
          settings (.-_settings eavt)
          meta     (merge
                     {:schema  (:schema db)
                      :max-eid (:max-eid db)
                      :max-tx  (:max-tx db)
                      :eavt    (set/store (:eavt db) adapter)
                      :aevt    (set/store (:aevt db) adapter)
                      :avet    (set/store (:avet db) adapter)}
                     (@#'set/settings->map settings))]
      (vswap! *store-buffer* conj! [root-addr meta])
      (-store (:storage adapter) (persistent! @*store-buffer*))
      db)))

(defn store!
  ([db]
   (if-some [adapter (adapter db)]
     (store-impl! db adapter {})
     (throw (ex-info "Database has no associated storage" {}))))
  ([db storage]
   (if-some [adapter (adapter db)]
     (let [current-storage (:storage adapter)]
       (if (identical? current-storage storage)
         (store-impl! db adapter {})
         (throw (ex-info "Database is already stored with another IStorage" {:storage current-storage}))))
     (let [settings (.-_settings ^PersistentSortedSet (:eavt db))
           adapter  (StorageAdapter. storage settings)]
       (store-impl! db adapter {})))))

(defn restore
  ([storage]
   (restore storage {}))
  ([storage opts]
   (let [root (-restore storage root-addr)
         {:keys [schema eavt aevt avet max-eid max-tx]} root
         opts    (merge root opts)
         adapter (make-adapter storage opts)]
     (db/restore-db
       {:schema  schema
        :eavt    (set/restore-by db/cmp-datoms-eavt eavt adapter opts)
        :aevt    (set/restore-by db/cmp-datoms-aevt aevt adapter opts)
        :avet    (set/restore-by db/cmp-datoms-avet avet adapter opts)
        :max-eid max-eid
        :max-tx  max-tx}))))

(defn- addresses-impl [db *set]
  (let [visit-fn #(vswap! *set conj! %)]
    (visit-fn root-addr)
    (.walkAddresses ^PersistentSortedSet (:eavt db) visit-fn)
    (.walkAddresses ^PersistentSortedSet (:aevt db) visit-fn)
    (.walkAddresses ^PersistentSortedSet (:avet db) visit-fn)))
  
(defn addresses
  "Returns all addresses in use by current db. Anything that is not in
   the return set is safe to be deleted"
  [& dbs]
  (let [*set (volatile! (transient #{}))]
    (doseq [db dbs]
      (addresses-impl db *set))
    (persistent! @*set)))

(defn collect-garbage!
  "Deletes all keys from storage that are not referenced by any of the provided dbs.
   Careful! If you have a lazy-loaded database and do GC on a newer version of it,
   old version might stop working. Make sure to always pass all alive references
   to DBs you are using"
  [& dbs]
  (let [used (apply addresses dbs)]
    (doseq [db dbs
            :let [storage (storage db)]
            :when storage
            :let [unused (->> (-list-addresses storage)
                           (remove used)
                           (vec))]]
      (-delete storage unused))))

(defn- output-stream
  "OutputStream that ignores flushes. Workaround for slow transit-clj JSON writer.
   See https://github.com/cognitect/transit-clj/issues/43"
  ^OutputStream [^File file]
  (let [os (FileOutputStream. file)]
    (proxy [BufferedOutputStream] [os]
      (flush [])
      (close []
        (let [this ^BufferedOutputStream this]
          (proxy-super flush)
          (proxy-super close))))))

(defn file-storage
  "Default implementation that stores data in files in a dir.
   
   Options are:
   
   :freeze-fn :: (data)   -> String. A serialization function
   :thaw-fn   :: (String) -> data. A deserialization function
   :write-fn  :: (OutputStream data) -> void. Implement your own writer to FileOutputStream
   :read-fn   :: (InputStream) -> Object. Implement your own reader from FileInputStream
   :addr->filename-fn :: (UUID) -> String. Construct file name from address
   :filename->addr-fn :: (String) -> UUID. Reconstruct address from file name
   
   All options are optional."
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
