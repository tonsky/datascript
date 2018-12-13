(ns datascript.test.cljs
  (:require
    [clojure.string :as str]
    [cljs.test :as t])
  #?(:cljs
      (:require-macros
        [datascript.test.cljs])))

; The datascript.test.cljs namespace exists only for the side
; effect of extending the cljs.test/assert-expr multimethod.

; This has to be done on the clj side of cljs compilation, and
; so we have a separate namespace that is only loaded by cljs
; via a :require-macros clause in datascript.test.core. This
; means we have a clj namespace that should only be loaded by
; cljs compilation.

#?(:clj
(defmethod t/assert-expr 'thrown-msg? [menv msg form]
  (let [[_ match & body] form]
    `(try
       ~@body
       (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
       (catch :default e#
         (let [m# (.-message e#)]
           (if (= ~match m#)
             (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
             (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#}))
           e#))))))