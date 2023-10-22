(ns datascript.conn
  (:require
    [datascript.db :as db #?@(:cljs [:refer [FilteredDB]])]
    [datascript.storage :as storage]
    [me.tonsky.persistent-sorted-set :as set])
  #?(:clj
     (:import
       [datascript.db FilteredDB])))

(defn with
  ([db tx-data] (with db tx-data nil))
  ([db tx-data tx-meta]
   {:pre [(db/db? db)]}
   (if (instance? FilteredDB db)
     (throw (ex-info "Filtered DB cannot be modified" {:error :transaction/filtered}))
     (db/transact-tx-data (db/->TxReport db db [] {} tx-meta) tx-data))))

(defn conn? [conn]
  (and #?(:clj  (instance? clojure.lang.IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
    (db/db? @conn)))

(defn conn-from-db [db]
  {:pre [(db/db? db)]}
  (if-some [storage (storage/storage db)]
    (do
      (storage/store db)
      (atom db 
        :meta {:listeners      (atom {})
               :tx-tail        (atom [])
               :db-last-stored (atom db)}))
    (atom db
      :meta {:listeners (atom {})})))

(defn conn-from-datoms
  ([datoms]
   (conn-from-db (db/init-db datoms nil {})))
  ([datoms schema]
   (conn-from-db (db/init-db datoms schema {})))
  ([datoms schema opts]
   (conn-from-db (db/init-db datoms schema (storage/maybe-adapt-storage opts)))))

(defn create-conn
  ([]
   (conn-from-db (db/empty-db nil {})))
  ([schema]
   (conn-from-db (db/empty-db schema {})))
  ([schema opts]
   (conn-from-db (db/empty-db schema (storage/maybe-adapt-storage opts)))))

#?(:clj
   (defn restore-conn
     ([storage]
      (restore-conn storage {}))
     ([storage opts]
      (when-some [[db tail] (storage/restore-impl storage opts)]
        (atom (storage/db-with-tail db tail)
          :meta {:listeners      (atom {})
                 :tx-tail        (atom tail)
                 :db-last-stored (atom db)})))))

(defn ^:no-doc -transact! [conn tx-data tx-meta]
  {:pre [(conn? conn)]}
  (let [*report (atom nil)]
    (swap! conn
      (fn [db]
        (let [r (with db tx-data tx-meta)]
          (reset! *report r)
          (:db-after r))))
    #?(:clj
       (when-some [storage (storage/storage @conn)]
         (let [{db     :db-after
                datoms :tx-data} @*report
               settings (set/settings (:eavt db))
               *tx-tail (:tx-tail (meta conn))
               tx-tail' (swap! *tx-tail conj datoms)]
           (if (> (transduce (map count) + 0 tx-tail') (:branching-factor settings))
             ;; overflow tail
             (do
               (storage/store-impl! db (storage/storage-adapter db) false)
               (reset! *tx-tail [])
               (reset! (:db-last-stored (meta conn)) db))
             ;; just update tail
             (storage/store-tail db tx-tail')))))
    @*report))

(defn transact!
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (let [report (-transact! conn tx-data tx-meta)]
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     report)))

(defn reset-conn!
  ([conn db]
   (reset-conn! conn db nil))
  ([conn db tx-meta]
   {:pre [(conn? conn)
          (db/db? db)]}
   (let [db-before @conn
         report    (db/map->TxReport
                     {:db-before db-before
                      :db-after  db
                      :tx-data   (concat
                                   (map #(assoc % :added false) (db/-datoms db-before :eavt nil nil nil nil))
                                   (db/-datoms db :eavt nil nil nil nil))
                      :tx-meta   tx-meta})]
     #?(:clj
        (when-some [storage (storage/storage db-before)]
          (storage/store db)
          (reset! (:tx-tail (meta conn)) [])
          (reset! (:db-last-stored (meta conn)) db)))
     (reset! conn db)
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     db)))

(defn reset-schema! [conn schema]
  {:pre [(conn? conn)]}
  (let [db (swap! conn db/with-schema schema)]
    #?(:clj
       (when-some [storage (storage/storage @conn)]
         (storage/store-impl! db (storage/storage-adapter db) true)
         (reset! (:tx-tail (meta conn)) [])
         (reset! (:db-last-stored (meta conn)) db)))
    db))

(defn- atom? [a]
  #?(:cljs (instance? Atom a)
     :clj  (instance? clojure.lang.IAtom a)))

(defn listen!
  ([conn callback]
   (listen! conn (rand) callback))
  ([conn key callback]
   {:pre [(conn? conn)
          (atom? (:listeners (meta conn)))]}
   (swap! (:listeners (meta conn)) assoc key callback)
   key))

(defn unlisten!
  [conn key]
  {:pre [(conn? conn)
         (atom? (:listeners (meta conn)))]}
  (swap! (:listeners (meta conn)) dissoc key))
