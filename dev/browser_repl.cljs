(ns browser-repl
  (:require
    [clojure.browser.repl :as repl]
     datascript.core))

(defonce conn
  (repl/connect "http://localhost:9000/repl"))

(enable-console-print!)
