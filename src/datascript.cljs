(ns datascript
  (:require
    [datascript.core :as dc]
    [datascript.query :as dq]
    [datascript.impl.entity :as de]
    [datascript.btset :as btset]))

;; SUMMING UP

(def  q dq/q)
(def  entity de/entity)
(defn entity-db [entity] (.-db entity))
(def  touch de/touch)

(def ^:const tx0 0x20000000)

(defn empty-db [& [schema]]
  (dc/DB. schema
       (btset/btset-by dc/cmp-datoms-eavt) 
       (btset/btset-by dc/cmp-datoms-aevt)
       (btset/btset-by dc/cmp-datoms-avet)
       0
       tx0))

(defn create-conn [& [schema]]
  (atom (empty-db schema)
        :meta { :listeners  (atom {}) }))

(defn transact [db entities]
  (dc/transact-entities (dc/TxReport. db db [] {}) entities))

(defn with [db entities]
  (:db-after (transact db entities)))

(defn -transact! [conn entities]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (transact db entities)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact! [conn entities]
  (let [report (-transact! conn entities)]
    (doseq [[_ callback] @(:listeners (meta conn))]
      (callback report))
    report))
           
(defn listen!
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
     (swap! (:listeners (meta conn)) assoc key callback)
     key))

(defn unlisten! [conn key]
  (swap! (:listeners (meta conn)) dissoc key))

(defn- components->pattern [index [c0 c1 c2 c3]]
  (case index
    :eavt (dc/Datom. c0 c1 c2 c3 nil)
    :aevt (dc/Datom. c1 c0 c2 c3 nil)
    :avet (dc/Datom. c2 c0 c1 c3 nil)))

(defn datoms [db index & cs]
  (btset/slice (get db index) (components->pattern index cs)))

(defn seek-datoms [db index & cs]
  (btset/slice (get db index) (components->pattern index cs) (dc/Datom. nil nil nil nil nil)))

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(extend-type dc/Datom
  IPrintWithWriter
  (-pr-writer [d w opts]
    (pr-sequential-writer w pr-writer "#datascript/Datom [" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)])))

(defn datom-from-reader [[e a v tx added]]
  (dc/Datom. e a v tx added))

(extend-type dc/DB
  IPrintWithWriter
  (-pr-writer [db w opts]
    (-write w "#datascript/DB {")
    (-write w ":schema ")
    (pr-writer (.-schema db) w opts)
    (-write w ", :datoms ")
    (pr-sequential-writer w
      (fn [d w opts]
        (pr-sequential-writer w pr-writer "[" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d)]))
      "[" " " "]" opts (.-eavt db))
    (-write w "}")))

(defn db-from-reader [{:keys [schema datoms]}]
  (let [datoms (map (fn [[e a v tx]] (dc/Datom. e a v tx true)) datoms)]
    (dc/DB. schema
         (apply btset/btset-by dc/cmp-datoms-eavt datoms)
         (apply btset/btset-by dc/cmp-datoms-aevt datoms)
         (apply btset/btset-by dc/cmp-datoms-avet datoms)
         (reduce max 0 (map :e datoms))
         (reduce max tx0 (map :tx datoms)))))
