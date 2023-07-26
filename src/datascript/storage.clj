
(in-ns 'datascript.core)

(require
  '[clojure.edn :as edn]
  '[clojure.java.io :as io]
  '[datascript.db :as db]
  '[me.tonsky.persistent-sorted-set :as set])

(import
  '[datascript.db Datom]
  '[java.util List]
  '[java.io BufferedOutputStream File FileOutputStream OutputStream PushbackReader] 
  '[me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet RefType Settings])

(def ^:private root-addr
  #uuid "d5695966-036d-6740-8541-d80034219c28")

(defprotocol IStorage
  (-store [_ addr data])
  (-restore [_ addr]))

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
   (let [name-fn   (or (:name-fn opts) str)
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
       (-store [_ addr data]
         (with-open [os (output-stream (io/file dir (name-fn addr)))]
           (write-fn os data)))
       (-restore [_ addr]
         (with-open [is (io/input-stream (io/file dir (name-fn addr)))]
           (read-fn is)))))))

(deftype StorageAdapter [^me.tonsky.persistent_sorted_set.IStorage istorage
                         ^Settings settings]
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
         adapter  (StorageAdapter. istorage settings)
         meta     (merge
                    {:schema   (:schema db)
                     :max-eid  (:max-eid db)
                     :max-tx   (:max-tx db)
                     :eavt     (set/store (:eavt db) adapter)
                     :aevt     (set/store (:aevt db) adapter)
                     :avet     (set/store (:avet db) adapter)}
                    (@#'set/settings->map settings))]
     (-store istorage root-addr meta))))

(defn restore
  ([istorage]
   (restore istorage {}))
  ([istorage opts]
   (let [root (-restore istorage root-addr)
         {:keys [schema eavt aevt avet max-eid max-tx]} root
         settings    (@#'set/map->settings root)
         adapter (StorageAdapter. istorage settings)]
     (db/restore-db
       {:schema  schema
        :eavt    (set/restore-by db/cmp-datoms-eavt eavt adapter root)
        :aevt    (set/restore-by db/cmp-datoms-aevt aevt adapter root)
        :avet    (set/restore-by db/cmp-datoms-avet avet adapter root)
        :max-eid max-eid
        :max-tx  max-tx}))))
