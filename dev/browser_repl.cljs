(ns browser-repl
  (:require
    [clojure.browser.repl :as repl]
     datascript))

(defonce conn
  (repl/connect "http://localhost:9000/repl"))

(enable-console-print!)
