(ns datascript.test.core
  (:require
    [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [clojure.string :as str]
    [datascript.core :as d]
    [datascript.impl.entity :as de]
    [datascript.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                              :clj  [:refer [defrecord-updatable]])]
    #?(:cljs [datascript.test.cljs])))

#?(:cljs
   (enable-console-print!))

;; Added special case for printing ex-data of ExceptionInfo
#?(:cljs
  (defmethod t/report [::t/default :error] [m]
    (t/inc-report-counter! :error)
    (println "\nERROR in" (t/testing-vars-str m))
    (when (seq (:testing-contexts (t/get-current-env)))
      (println (t/testing-contexts-str)))
    (when-let [message (:message m)] (println message))
    (println "expected:" (pr-str (:expected m)))
    (print "  actual: ")
    (let [actual (:actual m)]
      (cond
        (instance? ExceptionInfo actual)
          (println (.-stack actual) "\n" (pr-str (ex-data actual)))
        (instance? js/Error actual)
          (println (.-stack actual))
        :else
          (prn actual)))))

#?(:cljs (def test-summary (atom nil)))
#?(:cljs (defmethod t/report [::t/default :end-run-tests] [m]
           (reset! test-summary (dissoc m :type))))

(defn wrap-res [f]
  #?(:cljs (do (f) (clj->js @test-summary))
     :clj  (let [res (f)]
             (when (pos? (+ (:fail res) (:error res)))
               (System/exit 1)))))

;; utils
#?(:clj
(defmethod t/assert-expr 'thrown-msg? [msg form]
  (let [[_ match & body] form]
    `(try ~@body
          (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch Throwable e#
            (let [m# (.getMessage e#)]
              (if (= ~match m#)
                (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
                (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#})))
            e#)))))

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
                                  {:db/id (:db/id %)}
                                  %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eavt)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t))) 

;; Core tests

(deftest test-protocols
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        db (d/db-with (d/empty-db schema)
                      [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                       {:db/id 2 :name "Petr" :age 37 :huh? false}])]
    (is (= (d/empty-db schema)
           (empty db)))
    (is (= 6 (count db)))
    (is (= (set (seq db))
           #{(d/datom 1 :aka "IV")
             (d/datom 1 :aka "Terrible")
             (d/datom 1 :name "Ivan")
             (d/datom 2 :age 37)
             (d/datom 2 :name "Petr")
             (d/datom 2 :huh? false)}))
    ))
