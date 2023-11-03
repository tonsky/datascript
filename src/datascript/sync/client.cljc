(ns datascript.sync.client
  (:require
    [datascript.conn :as conn]
    [datascript.db :as db]
    [datascript.serialize :as serialize]
    [datascript.util :as util]))

(defn client-id []
  (long (* (rand) 0x1FFFFFFFFFFFFF)))

(def *last-tx-id
  (atom 0))

(defn new-tx-id [client-id]
  [client-id (swap! *last-tx-id inc)])

(defn on-tx [conn report]
  (when-not (:server? (:tx-meta report))
    (let [{:keys [client-id send-fn server-idx connected?]} @(:atom conn)
          tx {:tx-data (db/tx-from-datoms (:tx-data report))
              :tx-id   (new-tx-id client-id)}]
      (when connected?
        (send-fn
          {:message    :transacting
           :server-idx server-idx
           :txs        [tx]}))
      (swap! (:atom conn) update :pending conj tx))))

(defn create-conn [patterns send-fn]
  (let [res (@#'conn/make-conn
              {:db         nil
               :client-id  (client-id)
               :server-db  nil
               :pending    util/empty-queue
               :server-idx nil
               :connected? true
               :send-fn    send-fn})]
    (send-fn {:message :catching-up})
    res))

(defn server-message [conn body]
  (case (:message body)
    :catched-up
    (let [{:keys [snapshot server-idx]} body
          db (serialize/from-serializable snapshot)]
      (conn/reset-conn! conn db {:server? true})
      (swap! (:atom conn)
        (fn [atom]
          (-> atom
            (assoc :server-db db)
            (assoc :server-idx server-idx)
            (update :listeners assoc :sync #(on-tx conn %))))))
    
    :transacted
    (let [{:keys [tx-data tx-id server-idx]} body
          {server-db  :server-db
           pending    :pending
           listeners  :listeners} @(:atom conn)
          report     (conn/with server-db tx-data {:server? true})
          server-db' (:db-after report)]
      (swap! (:atom conn) assoc
        :server-db server-db'
        :server-idx server-idx)
      (if (= tx-id (:tx-id (peek pending)))
        (swap! (:atom conn) update :pending pop)
        (do
          (reset! conn (reduce conn/db-with server-db' pending))
          (doseq [[_ callback] listeners]
            (callback report))))))
  nil)

(defn server-disconnected [conn]
  (swap! (:atom conn)
    #(-> %
       (assoc :connected? false)
       (update :listeners dissoc :sync))))

(defn server-connected [conn]
  (swap! (:atom conn) assoc :connected? true)
  (let [{:keys [send-fn server-idx]} @(:atom conn)]
    (send-fn {:message    :catching-up
              :server-idx server-idx})))
