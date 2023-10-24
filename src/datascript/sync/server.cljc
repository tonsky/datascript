(ns datascript.sync.server
  (:require
    [datascript.conn :as conn]
    [datascript.db :as db]
    [datascript.serialize :as serialize]))

(defn- client [conn channel]
  (get @(:clients (meta conn)) channel))

(defn on-tx [conn report]
  ;; TODO filter what to send where
  (let [msg {:message    :transacted
             :tx-data    (db/tx-from-datoms (:tx-data report))
             :tx-id      (:tx-id (:tx-meta report))
             :server-idx (:db/current-tx (:tempids report))}]
    (doseq [[channel {:keys [status send-fn]}] @(:clients (meta conn))
            ; :let [_ (prn "broadcasting to" channel status)]
            :when (= :active status)]
      (send-fn channel msg))))

(defn client-connected [conn channel send-fn]
  (let [*clients (:clients (meta conn))
        clients' (swap! *clients assoc channel
                   {:status  :connected
                    :send-fn send-fn})]
    (when (= 1 (count clients'))
      (conn/listen! conn :sync #(on-tx conn %)))
    nil))

(defn client-message [conn channel body]
  (case (:message body)
    :catching-up
    (let [{:keys [patterns server-idx]} body ;; TODO delta from server-idx
          {:keys [send-fn]}             (client conn channel)
          db                            @conn]
      (send-fn channel
        {:message    :catched-up
         :snapshot   (serialize/serializable db) ;; TODO patterns
         :server-idx (:max-tx db)})
      ;; TODO race - external txs between (:max-tx db) and after :status :active
      (swap! (:clients (meta conn)) update channel assoc :status :active))
    
    :transacting
    (doseq [{:keys [tx-data tx-id]} (:txs body)]
      ;; TODO handle exception here
      (conn/transact! conn tx-data {:tx-id tx-id})))
  nil)

(defn client-disconnected [conn channel]
  (let [*clients (:clients (meta conn))
        clients' (swap! *clients dissoc channel)]
    (when (= 0 (count clients'))
      (conn/unlisten! conn :sync))
    nil))
