(ns datascript.bench.datomic
  (:require
   [clojure.string :as str]
   [datomic.api :as d]
   [datascript.bench.bench :as bench]))

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
          [ (schema-attr :id        :db.type/long, :db/unique :db.unique/identity)
            (schema-attr :name      :db.type/string)
            (schema-attr :last-name :db.type/string)
            (schema-attr :full-name :db.type/string)
            (schema-attr :sex       :db.type/keyword)
            (schema-attr :age       :db.type/long)
            (schema-attr :salary    :db.type/long)
            (schema-attr :alias     :db.type/string, :db/cardinality :db.cardinality/many)
            (schema-attr :follows   :db.type/ref, :db/cardinality :db.cardinality/many)])
        conn))))

(defn db-with [conn tx-data]
  (-> conn
      (d/transact tx-data)
      deref
      :db-after))

(def *db100k
  (delay
    (db-with (new-conn "db100k") @bench/*people20k)))

(defn tempid [i]
  (d/tempid :db.part/user (- i)))

(defn bench-add-1 []
  (bench/bench
    (let [conn (new-conn)]
      (doseq [p @bench/*people20k]
        (let [report @(d/transact conn [[:db/add "p" :name (:name p)]])
              id     (get (:tempids report) "p")]
          @(d/transact conn [[:db/add id :last-name (:last-name p)]])
          @(d/transact conn [[:db/add id :sex       (:sex p)]])
          @(d/transact conn [[:db/add id :age       (:age p)]])
          @(d/transact conn [[:db/add id :salary    (:salary p)]]))))))

(defn bench-add-5 []
  (bench/bench
    (let [conn (new-conn)]
      (doseq [p @bench/*people20k]
        @(d/transact conn [p])))))

(defn bench-add-all []
  (bench/bench
    (let [conn (new-conn)]
      @(d/transact conn @bench/*people20k))))

(defn bench-retract-5 []
  (bench/bench
    (let [conn (new-conn)
          db   (db-with conn @bench/*people20k)
          eids (->> (d/datoms db :aevt :name) (map :e) (shuffle))]
      (doseq [eid eids]
        @(d/transact conn [[:db.fn/retractEntity eid]])))))

(defn bench-q1 []
  (bench/bench
    (d/q '[:find ?e
           :where [?e :name "Ivan"]]
      @*db100k)))

(defn bench-q2 []
  (bench/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]]
      @*db100k)))

(defn bench-q3 []
  (bench/bench
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e :sex :male]]
      @*db100k)))

(defn bench-q4 []
  (bench/bench
    (d/q '[:find ?e ?l ?a
           :where [?e :name "Ivan"]
                  [?e :last-name ?l]
                  [?e :age ?a]
                  [?e :sex :male]]
      @*db100k)))

(defn bench-qpred1 []
  (bench/bench
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      @*db100k)))

(defn bench-qpred2 []
  (bench/bench
    (d/q '[:find ?e ?s
           :in   $ ?min_s
           :where [?e :salary ?s]
                  [(> ?s ?min_s)]]
      @*db100k 50000)))

(def *pull-db
  (delay
    (db-with (new-conn "pull-db") (bench/wide-db 1 4 5))))

(defn bench-pull-one []
  @*pull-db
  (bench/bench
    (d/pull @*pull-db [:name {:follows '...}] [:id 1])))

(defn bench-pull-one-entities []
  @*pull-db
  (let [f (fn f [entity]
            (assoc
              (select-keys entity [:name])
              :follows (mapv f (:follows entity))))]
    (bench/bench
      (f (d/entity @*pull-db [:id 1])))))

(defn bench-pull-many []
  @*pull-db
  (bench/bench
    (d/pull @*pull-db [:db/id :last-name :alias :sex :age :salary {:follows '...}] [:id 1])))

(defn bench-pull-many-entities []
  @*pull-db
  (let [f (fn f [entity]
            (assoc
              (select-keys entity [:db/id :last-name :alias :sex :age :salary])
              :follows (mapv f (:follows entity))))]
    (bench/bench
      (f (d/entity @*pull-db [:id 1])))))

(defn bench-pull-wildcard []
  @*pull-db
  (bench/bench
    (d/pull @*pull-db ['* {:follows '...}] [:id 1])))

(def benches
  {"add-1"              bench-add-1
   "add-5"              bench-add-5
   "add-all"            bench-add-all
   "retract-5"          bench-retract-5
   "q1"                 bench-q1
   "q2"                 bench-q2
   "q3"                 bench-q3
   "q4"                 bench-q4
   "qpred1"             bench-qpred1
   "qpred2"             bench-qpred2
   "pull-one"           bench-pull-one
   "pull-one-entities"  bench-pull-one-entities
   "pull-many"          bench-pull-many
   "pull-many-entities" bench-pull-many-entities
   "pull-wildcard"      bench-pull-wildcard})

(defn -main
  "clj -A:bench:datomic -M -m datascript.bench.datomic [--profile] (add-1 | add-5 | ...)*"
  [& args]
  (let [profile? (.contains (or args ()) "--profile")
        args     (remove #{"--profile"} args)
        names    (or (not-empty args) (sort (keys benches)))
        _        (apply println "Datomic:" names)
        longest  (last (sort-by count names))]
    (binding [bench/*profile* profile?]
      (doseq [name names
              :let [fn (benches name)]]
        (if (nil? fn)
          (println
            (bench/right-pad name (count longest))
            " ---")
          (let [{:keys [mean-ms file]} (fn)]
            (println
              (bench/right-pad name (count longest))
              " "
              (bench/left-pad (bench/round mean-ms) 6) "ms/op"
              " " (or file ""))))))
    (shutdown-agents)))

(comment
  (-main)
  (clojure.pprint/pprint (bench/wide-db 1 2 2))

  (bench-add-1)
  (bench-add-5)
  (bench-add-all)
  (bench-retract-5)
  (bench-q1)
  (bench-q2)
  (bench-q3)
  (bench-q4)
  (bench-qpred1)
  (bench-qpred2)
  (bench-pull-one)
  (bench-pull-one-entities)
  (bench-pull-many)
  (bench-pull-many-entities)
  (bench-pull-wildcard))