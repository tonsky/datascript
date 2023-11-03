(ns datascript.sync.server
  (:require
    [datascript.conn :as conn]
    [datascript.db :as db]
    [datascript.serialize :as serialize]))

(defn- client [conn channel]
  (-> conn :atom deref :clients (get channel)))

(defn on-tx [conn report]
  (let [clients  (:clients @(:atom conn))
        msg      {:message    :transacted
                  :tx-data    (db/tx-from-datoms (:tx-data report))
                  :tx-id      (:tx-id (:tx-meta report))
                  :server-idx (:db/current-tx (:tempids report))}]
    (doseq [[channel {:keys [status send-fn pending]}] clients]
      (if (= :active status)
        (do
          (when pending
            (doseq [msg pending]
              (send-fn channel msg))
            (swap! (:atom conn) update :clients update client dissoc :pending))
          (send-fn channel msg))
        (swap! (:atom conn) update :clients update client update :pending (fnil conj []) msg)))))

(defn client-connected [conn channel send-fn]
  (let [clients' (:clients
                   (swap! (:atom conn) update :clients assoc channel
                     {:status  :connected
                      :send-fn send-fn}))]
    (when (= 1 (count clients'))
      (conn/listen! conn :sync #(on-tx conn %)))
    nil))

(defn drop-before [txs server-idx]
  (vec
    (drop-while #(<= (:server-idx %) server-idx) txs)))

(defn client-message [conn channel body]
  (case (:message body)
    :catching-up
    (let [{:keys [patterns server-idx]} body ;; TODO delta from server-idx
          {:keys [send-fn]}             (client conn channel)
          db                            @conn
          server-idx                    (:max-tx db)]
      (send-fn channel
        {:message    :catched-up
         :snapshot   (serialize/serializable db) ;; TODO patterns
         :server-idx server-idx})
      (swap! (:atom conn) update :clients update channel
        (fn [client]
          (-> client
            (assoc :status :active)
            (update :pending drop-before server-idx)))))
    
    :transacting
    (doseq [{:keys [tx-data tx-id]} (:txs body)]
      ;; TODO handle exception here
      (conn/transact! conn tx-data {:tx-id tx-id})))
  nil)

(defn client-disconnected [conn channel]
  (let [clients' (:clients (swap! (:atom conn) update :clients dissoc channel))]
    (when (= 0 (count clients'))
      (conn/unlisten! conn :sync))
    nil))
