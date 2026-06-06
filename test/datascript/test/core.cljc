(ns datascript.test.core
  (:require
   [#?(:cljs cljs.reader :cljd cljd.reader :clj clojure.edn) :as edn]
   #?(:cljd  [cljd.test :as t :refer        [is are deftest testing]]
      :cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [clojure.string :as str]
   #?(:cljd [wevre.transit-cljd :as transit]
      :default [cognitect.transit :as transit])
   [datascript.core :as d]
   [datascript.impl.entity :as de]
   [datascript.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                             :clj  [:refer [defrecord-updatable]])]
   #?(:cljs [datascript.test.cljs])))

#?(:cljs
   (enable-console-print!))

#?(:cljd
   (defmacro thrown-msg? [expected-msg & body]
     `(try
        ~@body
        false
        (catch dynamic ^ExceptionInfo e#
          (or (.contains ^String (or (.-message ^ExceptionInfo e#) (.toString e#)) ~expected-msg)
              ;; rethrow for now to have a telling exception
              (throw e#))))))

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
  #?(:cljd :TOOD?
     :cljs (do (f) (clj->js @test-summary))
     :clj  (let [res (f)]
             (when (pos? (+ (:fail res) (:error res)))
               (System/exit 1)))))

;; utils
#?(:cljd nil
   :clj
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

#?(:cljd
   (defn no-namespace-maps
     ([])
     ([_]))
   :clj
(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))
:cljs
(def no-namespace-maps {:before #(set! *print-namespace-maps* false)}))

(defn transit-write [o type]
  #?(:cljd
     (let [json-enc (.-encoder (transit/json))
           jsonv-enc (.-encoder (transit/json-verbose))
           msgpack-enc (.-encoder (transit/msgpack))]
       (condp = type
         :json (.convert json-enc o)
         :json-verbose (.convert jsonv-enc o)
         :msgpack (.convert msgpack-enc o)
         (.convert json-enc o)))
     :clj
     (with-open [os (java.io.ByteArrayOutputStream.)]
       (let [writer (transit/writer os type)]
         (transit/write writer o)
         (.toByteArray os)))
     :cljs
     (transit/write (transit/writer type) o)))


(defn transit-write-str [o]
  #?(:cljd (transit-write o :json)
     :clj (String. ^bytes (transit-write o :json) "UTF-8")
     :cljs (transit-write o :json)))

(defn transit-read [s type]
  #?(:cljd
     (let [json-dec (.-decoder (transit/json))
           jsonv-dec (.-decoder (transit/json-verbose))
           msgpack-dec (.-decoder (transit/msgpack))]
       (condp = type
         :json (.convert json-dec s)
         :json-verbose (.convert jsonv-dec s)
         :msgpack (.convert msgpack-dec s)
         (.convert json-dec s)))
     :clj
     (with-open [is (java.io.ByteArrayInputStream. s)]
       (transit/read (transit/reader is type)))
     :cljs
     (transit/read (transit/reader type) s)))

(defn transit-read-str [s]
  #?(:cljd (transit-read s :json)
     :clj  (transit-read (.getBytes ^String s "UTF-8") :json)
     :cljs (transit-read s :json)))

;; Core tests

(deftest test-protocols
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        db (d/db-with (d/empty-db schema)
                      [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                       {:db/id 2 :name "Petr" :age 37 :huh? false}])]
    (is (= (d/empty-db schema)
           (empty db)))
    (is (= 6 (count db)))
    (is (= #{:schema :eavt :aevt :avet :max-eid :max-tx :rschema :pull-patterns :pull-attrs :hash}
          (set (keys db))))
    (is (map? db))
    (is (seqable? (:eavt db)))
    (is (= (set (seq (:eavt db)))
          #{(d/datom 1 :aka "IV")
            (d/datom 1 :aka "Terrible")
            (d/datom 1 :name "Ivan")
            (d/datom 2 :age 37)
            (d/datom 2 :name "Petr")
            (d/datom 2 :huh? false)}))))
