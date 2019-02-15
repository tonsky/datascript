(ns datascript-bench.datomic
  (:require
    [clojure.string :as str]
    [datomic.api :as d]
    [datomic.btset :as btset]
    [datascript-bench.core :as core]))

;; test-db

;; tests

(defn- schema-attr [name type & {:as args}]
  (merge
    {:db/id          (d/tempid :db.part/db)
     :db/ident       name
     :db/valueType   type
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    args))


(defn new-conn
  ([] (new-conn "bench"))
  ([name]
    (let [url (str "datomic:mem://" name)]
      (d/delete-database url)
      (d/create-database url)
      (let [conn (d/connect url)]
        @(d/transact conn
          [ (schema-attr :name      :db.type/string)
            (schema-attr :last-name :db.type/string)
            (schema-attr :sex       :db.type/keyword)
            (schema-attr :age       :db.type/long)
            (schema-attr :salary    :db.type/long)
            (schema-attr :follows   :db.type/ref, :db/cardinality :db.cardinality/many)])
        conn))))


(defn db-with [conn tx-data]
  (-> conn
      (d/transact tx-data)
      deref
      :db-after))


(def db100k
  (db-with (new-conn "db100k") core/people20k))


(defn tempid [i]
  (d/tempid :db.part/user (- i)))


(defn- wide-db
  ([depth width] (db-with (new-conn) (wide-db 1 depth width)))
  ([id depth width]
    (if (pos? depth)
      (let [children (map #(+ (* id width) %) (range width))]
        (concat
          (map #(array-map
                 :db/id   (tempid id)
                 :name    "Ivan"
                 :follows (tempid %)) children)
          (mapcat #(wide-db % (dec depth) width) children)))
      [{:db/id (tempid id) :name "Ivan"}])))

(defn- long-db [depth width]
  (db-with
    (new-conn)
    (apply concat
      (for [x (range width)
            y (range depth)
            :let [from (+ (* x (inc depth)) y)
                  to   (+ (* x (inc depth)) y 1)]]
        [{:db/id   (tempid from)
          :name    "Ivan"
          :follows (tempid to)}
         {:db/id   (tempid to)
          :name    "Ivan"}]))))


(defn ^:export add-1 []
  (core/bench
    (let [conn (new-conn)]
      (doseq [p core/people20k]
        (let [report @(d/transact conn [[:db/add "p" :name (:name p)]])
              id     (get (:tempids report) "p")]
          @(d/transact conn [[:db/add id :last-name (:last-name p)]])
          @(d/transact conn [[:db/add id :sex       (:sex p)]])
          @(d/transact conn [[:db/add id :age       (:age p)]])
          @(d/transact conn [[:db/add id :salary    (:salary p)]]))))))


(defn ^:export add-5 []
  (core/bench
    (let [conn (new-conn)]
      (doseq [p core/people20k]
        @(d/transact conn [p])))))


(defn ^:export add-all []
  (core/bench
    (let [conn (new-conn)]
      @(d/transact conn core/people20k))))


(defn ^:export retract-5 []
  (core/bench
    (let [conn (new-conn)
          db   (db-with conn core/people20k)
          eids (->> (d/datoms db :aevt :name) (map :e) (shuffle))]
      (doseq [eid eids]
        @(d/transact conn [[:db.fn/retractEntity eid]])))))


(defn ^:export q1 []
  (core/bench
    (d/q '[:find ?e
           :where [?e :name "Ivan"]]
      db100k)))

(defn ^:export q2 []
  (core/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]]
      db100k)))

(defn ^:export q3 []
  (core/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e :sex :male]]
      db100k)))

(defn ^:export q4 []
  (core/bench
    (d/q '[:find ?e ?l ?a
           :where [?e :name "Ivan"]
                  [?e :last-name ?l]
                  [?e :age ?a]
                  [?e :sex :male]]
      db100k)))

(defn ^:export qpred1 []
  (core/bench
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      db100k)))

(defn ^:export qpred2 []
  (core/bench
    (d/q '[:find ?e ?s
           :in   $ ?min_s
           :where [?e :salary ?s]
                  [(> ?s ?min_s)]]
      db100k 50000)))


(defn ^:export -main [& names]
  (doseq [n names]
    (if-some [benchmark (ns-resolve 'datascript-bench.datomic (symbol n))]
      (let [perf (benchmark)]
        (print (core/round perf) "\t")
        (flush))
      (do
        (print "---" "\t")
        (flush))))
  (println))
