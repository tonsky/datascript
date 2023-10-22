(ns datascript.storage)

(defn storage [db]
  nil)

(defn store
  ([db]
   (throw (ex-info "Not implemented: (storage/store db)" {})))
  ([db storage]
   (throw (ex-info "Not implemented: (storage/store db storage)" {}))))

(defn maybe-adapt-storage [opts]
  opts)
