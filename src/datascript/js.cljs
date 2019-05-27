(ns ^:no-doc datascript.js
  (:refer-clojure :exclude [filter])
  (:require
   [cljs.reader]
   [goog.object :as go]
   [clojure.walk :as walk]
   [datascript.core :as d]))

;; Conversions

(defn- keywordize [s]
  (if (and (string? s) (= (subs s 0 1) ":"))
    (keyword (subs s 1))
    s))

(defn- schema->clj [schema]
  (->> (js->clj schema)
       (reduce-kv
         (fn [m k v] (assoc m k (walk/postwalk keywordize v))) {})))

(declare entities->clj)

(defn- entity-map->clj [e]
  (walk/postwalk
    (fn [form]
      (if (and (map? form) (contains? form ":db/id"))
        (-> form
            (dissoc ":db/id")
            (assoc  :db/id (get form ":db/id")))
        form))
    e))

(defn- entity->clj [e]
  (cond
    (map? e)
    (entity-map->clj e)

    (= (first e) ":db.fn/call")
    (let [[_ f & args] e]
      (concat [:db.fn/call (fn [& args] (entities->clj (apply f args)))] args))

    (sequential? e)
    (let [[op & entity] e]
      (concat [(keywordize op)] entity))))

(defn- entities->clj [entities]
  (->> (js->clj entities)
       (map entity->clj)))

(defn- tempids->js [tempids]
  (let [obj (js-obj)]
    (doseq [[k v] tempids]
      (go/set obj (str k) v))
    obj))

(defn- tx-report->js [report]
  #js { :db_before (:db-before report)
        :db_after  (:db-after report)
        :tx_data   (->> (:tx-data report) into-array)
        :tempids   (tempids->js (:tempids report))
        :tx_meta   (:tx-meta report) })

(defn js->Datom [d]
  (if (array? d)
    (d/datom (aget d 0) (aget d 1) (aget d 2) (or (aget d 3) d/tx0) (or (aget d 4) true))
    (d/datom (.-e d) (.-a d) (.-v d) (or (.-tx d) d/tx0) (or (.-added d) true))))

(defn- pull-result->js
  [result]
  (->> result
       (walk/postwalk #(if (keyword? %) (str %) %))
       clj->js))

;; Public API

(defn ^:export empty_db [& [schema]]
  (d/empty-db (schema->clj schema)))

(defn ^:export init_db [datoms & [schema]]
  (d/init-db (map js->Datom datoms) (schema->clj schema)))

(defn ^:export q [query & sources]
  (let [query   (cljs.reader/read-string query)
        results (apply d/q query sources)]
    (clj->js results)))

(defn ^:export pull [db pattern eid]
  (let [pattern (cljs.reader/read-string pattern)
        eid (js->clj eid)
        results (d/pull db pattern eid)]
    (pull-result->js results)))

(defn ^:export pull_many [db pattern eids]
  (let [pattern (cljs.reader/read-string pattern)
        eids (js->clj eids)
        results (d/pull-many db pattern eids)]
    (pull-result->js results)))

(defn ^:export db_with [db entities]
  (d/db-with db (entities->clj entities)))

(defn ^:export entity [db eid]
  (d/entity db (js->clj eid)))

(def ^:export touch d/touch)
(def ^:export entity_db d/entity-db)
(def ^:export filter d/filter)
(def ^:export is_filtered d/is-filtered)

(defn ^:export create_conn [& [schema]]
  (d/create-conn (schema->clj schema)))

(def ^:export conn_from_db d/conn-from-db)

(defn ^:export conn_from_datoms
  ([datoms]        (conn_from_db (init_db datoms)))
  ([datoms schema] (conn_from_db (init_db datoms schema))))

(defn ^:export db [conn] @conn)

(defn ^:export transact [conn entities & [tx-meta]]
  (let [entities (entities->clj entities)
        report   (-> (d/-transact! conn entities tx-meta)
                     tx-report->js)]
    (doseq [[_ callback] @(:listeners (meta conn))]
      (callback report))
    report))

(defn ^:export reset_conn [conn db & [tx-meta]]
  (let [report #js { :db_before @conn
                     :db_after  db
                     :tx_data   (into-array
                                  (concat
                                    (map #(assoc % :added false) (d/datoms @conn :eavt))
                                    (d/datoms db :eavt)))
                     :tx_meta   tx-meta }]
    (reset! conn db)
    (doseq [[_ callback] @(:listeners (meta conn))]
      (callback report))
    db))

(def ^:export listen d/listen!)
(def ^:export unlisten d/unlisten!)

(defn ^:export resolve_tempid [tempids tempid]
  (go/get tempids (str tempid)))

(defn ^:export datoms [db index & components]
  (->> (apply d/datoms db (keywordize index) components)
       into-array))

(defn ^:export seek_datoms [db index & components]
  (->> (apply d/seek-datoms db (keywordize index) components)
       into-array))

(defn ^:export index_range [db attr start end]
  (into-array (d/index-range db attr start end)))

(defn ^:export squuid []
  (str (d/squuid)))

(defn ^:export squuid_time_millis [uuid]
  (d/squuid-time-millis (cljs.core/uuid uuid)))
