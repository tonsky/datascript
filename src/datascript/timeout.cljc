(ns ^:no-doc datascript.timeout)

(def ^:dynamic *deadline*
  "When non nil, query or pull will throw if its not done before *deadline* -- as returned by (System/currentTimeMillis) or (.now js/Date)"
  nil)

(defn to-deadline
  "Converts a timeout in milliseconds (or nil) to a deadline (or nil)."
  [timeout-in-ms]
  (some-> timeout-in-ms
          (#(+ ^long %
               #?(:clj ^long (System/currentTimeMillis)
                  :cljs (.now js/Date))))))

(defn assert-time-left
  "Throws if timeout exceeded"
  []
  (when (some-> *deadline*
                (#(< ^long %
                     #?(:clj ^long (System/currentTimeMillis)
                        :cljs (.now js/Date)))))
    (throw
     (ex-info "Query and/or pull expression took too long to run."
              {}))))
