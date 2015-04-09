(ns user
  (:require [cemerick.austin.repls :as repls]))

(defn repl-chrome []
  (repls/exec :exec-cmds ["open" "-ga" "/Applications/Google Chrome.app"]))

(defn repl-phantomjs []
  (repls/cljs-repl (cemerick.austin/exec-env)))
