(ns datascript.js
  (:require
    [datascript :as d]
    [cljs.reader :refer [read-string]]))

(enable-console-print!) ;; TODO

(defn- keywordize [s]
  (if (= (subs s 0 1) ":")
    (keyword (subs s 1))
    s))

(defn ^:export empty_db [& [schema]]
  (d/empty-db nil))

(defn ^:export q [query & sources]
  (let [query   (read-string query) ;; TODO support array & object form
        results (apply d/q query sources)]
    (->> (for [tuple results]
           (apply array tuple))
         (apply array))))

(defn- entity->clj [e]
  (cond (map? e)
    (-> e
      (dissoc ":db/id")
      (assoc  :db/id (e ":db/id")))
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
        :tx_data   (->> (:tx-data report) (map datom->js) (apply array))
        :tempids   (clj->js (:tempids report)) })

(defn ^:export with_datoms [db entities]
  (d/with db (entities->clj entities)))

(defn entity->js [e]
  (-> e
    (dissoc :db/id)
    (assoc  ":db/id" (:db/id e))
    clj->js))

(defn ^:export entity [db eid]
  (-> (d/entity db eid)
      entity->js))

;; Mutations

(defn ^:export create_conn [& [schema]]
  (d/create-conn nil)) ;; TODO support schema

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

