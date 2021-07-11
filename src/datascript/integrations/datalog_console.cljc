(ns datascript.integrations.datalog-console
  #?(:cljs (:require [goog.object :as gobj]
                     [goog.labs.userAgent.browser :as user-agent]
                     [cljs.reader])))


(defn init!
  [{:keys [conn]}]
  #?(:cljs (when (seq (user-agent/getVersion))
                 (js/document.documentElement.setAttribute "__datalog-console-remote-installed__" true)
                 (.addEventListener js/window "message"
                                    (fn [event]
                                      (when-let [devtool-message (gobj/getValueByKeys event "data" ":datalog-console.client/devtool-message")]
                                        (let [msg-type (:type (cljs.reader/read-string devtool-message))]
                                          (case msg-type

                                            :datalog-console.client/request-whole-database-as-string
                                            (.postMessage js/window #js {":datalog-console.remote/remote-message" (pr-str @conn)} "*")

                                            nil))))))))