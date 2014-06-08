(ns datascript.js
  (:require
    [datascript :as d]
    [clojure.walk :as walk]
    [cljs.reader :refer [read-string]]))

;; Conversions

(defn- keywordize [s]
  (if (and (string? s) (= (subs s 0 1) ":"))
    (keyword (subs s 1))
    s))

(defn- schema->clj [schema]
  (->> (js->clj schema)
       (walk/postwalk keywordize)))

(declare entities->clj)

(defn- entity->clj [e]
  (cond (map? e)
    (-> e
      (dissoc ":db/id")
      (assoc  :db/id (e ":db/id")))
    (= (first e) ":db.fn/call")
      (let [[_ f & args] e]
        (concat [:db.fn/call (fn [& args] (entities->clj (apply f args)))] args))
    (sequential? e)
      (let [[op & entity] e]
        (concat [(keywordize op)] entity))))

(defn- entities->clj [entities]
  (->> (js->clj entities)
       (map entity->clj)))

(defn- datom->js [d]
  #js { :e (.-e d)
        :a (.-a d)
        :v (.-v d)
        :tx (.-tx d)
        :added (.-added d) })

(defn- tx-report->js [report]
  #js { :db_before (:db-before report)
        :db_after  (:db-after report)
        :tx_data   (->> (:tx-data report) (map datom->js) into-array)
        :tempids   (clj->js (:tempids report)) })

(defn entity->js [e]
  (-> e
    (dissoc :db/id)
    (assoc  ":db/id" (:db/id e))
    clj->js))

;; Public API

(defn ^:export empty_db [& [schema]]
  (d/empty-db (schema->clj schema)))

(defn ^:export q [query & sources]
  (let [query   (read-string query)
        results (apply d/q query sources)]
    (->> (for [tuple results]
           (into-array tuple))
         (into-array))))

(defn ^:export with_datoms [db entities]
  (d/with db (entities->clj entities)))

(defn ^:export entity [db eid]
  (-> (d/entity db eid)
      entity->js))

(defn ^:export create_conn [& [schema]]
  (d/create-conn (schema->clj schema)))

(defn ^:export db [conn]
  @conn)

(defn ^:export transact [conn entities]
  (let [entities (entities->clj entities)
        report   (-> (d/-transact! conn entities)
                     tx-report->js)]
    (doseq [[_ callback] @(:listeners (meta conn))]
      (callback report))
    report))

(def ^:export listen d/listen!)

(def ^:export unlisten d/unlisten!)

(defn ^:export datoms [db index & components]
  (->> (apply d/datoms db (keywordize index) components)
       (map datom->js)
       into-array))

(defn ^:export seek_datoms [db index & components]
  (->> (apply d/seek-datoms db (keywordize index) components)
       (map datom->js)
       into-array))
