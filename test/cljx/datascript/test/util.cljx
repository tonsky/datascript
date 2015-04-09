(ns datascript.test.util
  #+cljs
  (:require-macros
    [cemerick.cljs.test :refer [with-test-out]])
  (:require
    #+cljs [cemerick.cljs.test :as t]
    #+clj [clojure.test :as t :refer [with-test-out]]
    [datascript :as d]
    )

  #+clj
  (:import [datascript.impl.entity Entity]))

#+cljs
(enable-console-print!)

#+cljs
(def Throwable js/Error)

;; Added special case for printing ex-data of ExceptionInfo
(defmethod t/report :error [{:keys [test-env] :as m}]
  #+cljs
  (with-test-out test-env
   (t/inc-report-counter test-env :error)
   (println "\nERROR in" (t/testing-vars-str m))
   (when (seq (::test-contexts @test-env))
      (println (t/testing-contexts-str test-env)))
   (when-let [message (:message m)] (println message))
   (println "expected:" (pr-str (:expected m)))
   (print "  actual: ")
   (let [actual (:actual m)]
     (cond
       (instance? ExceptionInfo actual) (println (.-stack actual) "\n" (ex-data actual))
       (instance? Throwable actual)     (println (.-stack actual))
       :else                            (prn actual))))
  #+clj
  (with-test-out
    (t/inc-report-counter :error)
    (println "\nERROR in" (t/testing-vars-str m))
    (when (seq t/*testing-contexts*) (println (t/testing-contexts-str)))
    (when-let [message (:message m)] (println message))
    (println "expected:" (pr-str (:expected m)))
    (print "  actual: ")
    (let [actual (:actual m)]
      (cond
        (instance? clojure.lang.ExceptionInfo actual) (do
                                                        (clojure.stacktrace/print-cause-trace actual t/*stack-trace-depth*)
                                                        (println (str "\n" (ex-data actual))))
        (instance? java.lang.Throwable actual)        (clojure.stacktrace/print-cause-trace actual t/*stack-trace-depth*)
        :else                                         (prn actual)))))

;; utils

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/postwalk (fn [x]
                                  (if (instance? Entity x)
                                    {:db/id (:db/id x)}
                                    x))))))
