(ns datascript.sync.client
  (:require
    [datascript.conn :as conn]
    [datascript.db :as db]
    [datascript.serialize :as serialize]))

(defn client-id []
  (long (* (rand) 0x1FFFFFFFFFFFFF)))

(def *last-tx-id
  (atom 0))

(defn new-tx-id [client-id]
  [client-id (swap! *last-tx-id inc)])

(defn on-tx [conn report]
  (when-not (:server? (:tx-meta report))
    (let [{:keys [client-id send-fn server-idx pending]} (meta conn)
          tx {:tx-data (db/tx-from-datoms (:tx-data report))
              :tx-id   (new-tx-id client-id)}]
      (send-fn
        {:message    :transacting
         :server-idx @server-idx
         :txs        [tx]})
      (swap! pending conj tx))))

(defn create-conn [patterns send-fn]
  (let [res (atom nil :meta
              {:client-id  (client-id)
               :server-db  (atom nil)
               :pending    (atom #?(:clj  clojure.lang.PersistentQueue/EMPTY
                                    :cljs cljs.core.PersistentQueue.EMPTY))
               :server-idx (atom nil)
               :send-fn    send-fn
               :listeners  (atom {})})]
    (send-fn {:message :catching-up})
    res))

(defn server-message [conn body]
  (case (:message body)
    :catched-up
    (let [{:keys [snapshot server-idx]} body
          db (serialize/from-serializable snapshot)]
      (reset! conn db)
      (reset! (:server-db (meta conn)) db)
      (reset! (:server-idx (meta conn)) server-idx)
      (conn/listen! conn :sync #(on-tx conn %)))
    
    :transacted
    (let [{:keys [tx-data tx-id server-idx]} body
          {*server-db  :server-db
           *server-idx :server-idx
           *pending    :pending
           *listeners  :listeners} (meta conn)
          report     (conn/with @*server-db tx-data {:server? true})
          server-db' (:db-after report)]
      (reset! *server-db server-db')
      (reset! *server-idx server-idx)
      (if (= tx-id (:tx-id (peek @*pending)))
        (swap! *pending pop)
        (do
          (reset! conn (reduce conn/db-with server-db' @*pending))
          (doseq [[_ callback] @*listeners]
            (callback report))))))
  nil)

(defn server-disconnected [conn]
  ;; TODO impl me
  )

(defn server-connected [conn]
  ;; TODO impl me
  )
