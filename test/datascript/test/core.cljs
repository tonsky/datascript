(ns datascript.test.core
  (:require-macros
    [cemerick.cljs.test :refer [with-test-out]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript :as d]))

(enable-console-print!)

;; See http://dev.clojure.org/jira/browse/CLJS-985
(defn -ex-info
  ([msg data]
    (set! (.-prototype ExceptionInfo) (js/Error msg))
    (set! (.. ExceptionInfo -prototype -name) "ExceptionInfo")
    (set! (.. ExceptionInfo -prototype -constructor) ExceptionInfo)
    (ExceptionInfo. msg data nil))
  ([msg data cause]
    (set! (.-prototype ExceptionInfo) (js/Error msg))
    (set! (.. ExceptionInfo -prototype -name) "ExceptionInfo")
    (set! (.. ExceptionInfo -prototype -constructor) ExceptionInfo)
    (ExceptionInfo. msg data cause)))

(set! cljs.core/ex-info -ex-info)

;; Added special case for printing ex-data of ExceptionInfo
(defmethod t/report :error [{:keys [test-env] :as m}]
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
       (instance? ExceptionInfo actual)
         (println (.-stack actual) "\n" (ex-data actual))
       (instance? js/Error actual)
         (println (.-stack actual))
       :else
         (prn actual)))))

;; utils

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/postwalk #(if (instance? datascript.impl.entity/Entity %)
                                     {:db/id (:db/id %)}
                                     %)))))
