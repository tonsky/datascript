(ns datascript.test.sync
  (:require
    [clojure.core.async :as async :refer [>! <! go go-loop]]
    [clojure.edn :as edn]
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]
    [datascript.sync.server :as server]
    [datascript.sync.client :as client]
    [datascript.test.core :as tdc]))

(defn wait-on [*atom cond]
  (let [*p (promise)]
    (add-watch *atom :wait
      (fn [_ _ _ new]
        (when (cond new)
          (deliver *p true))))
    (when (cond @*atom)
      (deliver *p true))
    (let [p (deref *p 100 :timeout)]
      (remove-watch *atom :wait)
      (when (= :timeout p)
        (throw (ex-info "Timeout" {}))))))

(def freeze
  ; identity
  tdc/transit-write-str
  )

(def thaw
  ; identity
  tdc/transit-read-str
  )

(defn setup []
  (let [server (d/create-conn)
        ch     (async/chan 10)
        ch1    (async/chan 10)
        ch2    (async/chan 10)
        c1     (client/create-conn nil #(go (>! ch [:c1 (freeze %)])))
        c2     (client/create-conn nil #(go (>! ch [:c2 (freeze %)])))]
    (server/client-connected server :c1 (fn [_ msg] (go (>! ch1 (freeze msg)))))
    (server/client-connected server :c2 (fn [_ msg] (go (>! ch2 (freeze msg)))))
    (go-loop []
      (when-some [msg (<! ch1)]
        (let [msg (thaw msg)]
          (tdc/log "C1 RCV" msg)
          (client/server-message c1 msg)
          (recur))))
    (go-loop []
      (when-some [msg (<! ch2)]
        (let [msg (thaw msg)]
          (tdc/log "C2 RCV" msg)
          (client/server-message c2 msg)
          (recur))))
    (go-loop []
      (when-some [[id msg] (<! ch)]
        (let [msg (thaw msg)]
          (tdc/log "SRV RCV" id msg)
          (server/client-message server id msg)
          (recur))))
    (wait-on c1 some?)
    (wait-on c2 some?)
    {:server server
     :c1     c1
     :c2     c2}))

(defn wait-all [{:keys [server c1 c2]}]
  (wait-on (:pending (meta c1)) empty?)
  (wait-on (:pending (meta c2)) empty?)
  (wait-on (:server-idx (meta c1)) #(= % (:max-tx @server)))
  (wait-on (:server-idx (meta c2)) #(= % (:max-tx @server))))

(deftest test-sync
  (let [{:keys [server c1 c2] :as setup} (setup)]
    (d/transact! c1 [[:db/add 1 :name "Ivan"]])
    (wait-all setup)
    (is (= #{[1 :name "Ivan"]}
          (tdc/all-datoms @c1)))
    (is (= #{[1 :name "Ivan"]}
          (tdc/all-datoms @c2)))
    (is (= #{[1 :name "Ivan"]}
          (tdc/all-datoms @server)))))
  

; (t/test-ns *ns*)
; (t/run-test-var #'test-conn)

