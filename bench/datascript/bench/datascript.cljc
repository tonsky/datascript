(ns datascript.bench.datascript
  (:require
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.bench.bench :as bench]
    [datascript.test]
    [me.tonsky.persistent-sorted-set :as pss]
    [me.tonsky.persistent-sorted-set.arrays :as arrays]
    #?(:clj [jsonista.core :as jsonista])
    #?(:cljs [goog.object :as gobj])))

#?(:cljs (enable-console-print!))

#?(:cljs
   (def fs (js/require "fs")))

(def schema
  {:id      {:db/unique :db.unique/identity}
   :follows {:db/valueType   :db.type/ref
             :db/cardinality :db.cardinality/many}
   :alias   {:db/cardinality :db.cardinality/many}})

(def empty-db (d/empty-db schema))

(def *db100k
  (delay
    (d/db-with (d/empty-db schema) @bench/*people20k)))

(defn wide-db [depth width]
  (d/db-with empty-db (bench/wide-db 1 depth width)))

(defn long-db [depth width]
  (d/db-with empty-db (bench/long-db depth width)))

(defn bench-add-1 []
  (bench/bench
    (reduce
      (fn [db p]
        (-> db
          (d/db-with [[:db/add (:db/id p) :name      (:name p)]])
          (d/db-with [[:db/add (:db/id p) :last-name (:last-name p)]])
          (d/db-with [[:db/add (:db/id p) :sex       (:sex p)]])
          (d/db-with [[:db/add (:db/id p) :age       (:age p)]])
          (d/db-with [[:db/add (:db/id p) :salary    (:salary p)]])))
      empty-db
      @bench/*people20k)))

(defn bench-add-5 []
  (bench/bench
    (reduce
      (fn [db p]
        (d/db-with db [p]))
      empty-db
      @bench/*people20k)))

(defn bench-add-all []
  (bench/bench
    (d/db-with
      empty-db
      @bench/*people20k)))

(defn bench-init []
  (let [datoms (into []
                 (for [p @bench/*people20k
                       :let [id (#?(:clj Integer/parseInt :cljs js/parseInt) (:db/id p))]
                       [k v] p
                       :when (not= k :db/id)]
                   (d/datom id k v)))]
    (bench/bench
      (d/init-db datoms))))

(defn bench-retract-5 []
  (let [db   (d/db-with empty-db @bench/*people20k)
        eids (->> (d/datoms db :aevt :name) (map :e) (shuffle))]
    (bench/bench
      (reduce (fn [db eid] (d/db-with db [[:db.fn/retractEntity eid]])) db eids))))

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
    (wide-db 4 5)))

(defn bench-pull-one-entities []
  (let [f (fn f [entity]
            (assoc
              (select-keys entity [:name])
              :follows (mapv f (:follows entity))))]
    (bench/bench
      (f (d/entity @*pull-db [:id 1])))))

(defn bench-pull-one []
  (bench/bench
    (d/pull @*pull-db [:name {:follows '...}] 1)))

(defn bench-pull-many-entities []
  (let [f (fn f [entity]
            (assoc
              (select-keys entity [:db/id :last-name :alias :sex :age :salary])
              :follows (mapv f (:follows entity))))]
    (bench/bench
      (f (d/entity @*pull-db [:id 1])))))

(defn bench-pull-many []
  (bench/bench
    (d/pull @*pull-db [:db/id :last-name :alias :sex :age :salary {:follows '...}] 1)))

(defn bench-pull-wildcard []
  (bench/bench
    (d/pull @*pull-db ['* {:follows '...}] 1)))

(defn bench-rules [db]
  (d/q '[:find ?e ?e2
         :in   $ %
         :where (follows ?e ?e2)]
    db
    '[[(follows ?x ?y)
       [?x :follows ?y]]
      [(follows ?x ?y)
       [?x :follows ?t]
       (follows ?t ?y)]]))

(defn bench-rules-wide-3x3 []
  (let [db (wide-db 3 3)]
    (bench/bench (bench-rules db))))

(defn bench-rules-wide-5x3 []
  (let [db (wide-db 5 3)]
    (bench/bench (bench-rules db))))

(defn bench-rules-wide-7x3 []
  (let [db (wide-db 7 3)]
    (bench/bench (bench-rules db))))

(defn bench-rules-wide-4x6 []
  (let [db (wide-db 4 6)]
    (bench/bench (bench-rules db))))

(defn bench-rules-long-10x3 []
  (let [db (long-db 10 3)]
    (bench/bench (bench-rules db))))

(defn bench-rules-long-30x3 []
  (let [db (long-db 30 3)]
    (bench/bench (bench-rules db))))

(defn bench-rules-long-30x5 []
  (let [db (long-db 30 5)]
    (bench/bench (bench-rules db))))

(def *ids10k
  (delay 
    (->> (d/datoms @*db100k :aevt :salary)
      (map :e)
      (distinct)
      (shuffle)
      (take 10000)
      (vec))))

(defn bench-slice-eavt []
  (bench/bench
    (doseq [id @*ids10k]
      (-> (d/datoms @*db100k :eavt id :namespaced/full-name) first :v))))

(defn bench-slice-aevt []
  (bench/bench
    (doseq [id @*ids10k]
      (-> (d/datoms @*db100k :aevt :namespaced/full-name id) first :v))))

(defn bench-subslice-aevt-cp []
  (bench/bench
    (doseq [:let [db    @*db100k
                  slice (d/datoms db :aevt :namespaced/full-name)]
            id @*ids10k
            :let [from (db/components->pattern db :aevt :namespaced/full-name id nil nil db/e0 db/tx0)
                  to   (db/components->pattern db :aevt :namespaced/full-name id nil nil db/emax db/txmax)]]
      (-> (pss/slice slice from to db/cmp-datoms-evt) first :v))))

(defn bench-seek []
  (bench/bench
    (doseq [:let [db   @*db100k]
            id @*ids10k
            :let [from (db/datom id :namespaced/full-name nil db/tx0)
                  to   (db/datom id :namespaced/full-name nil db/txmax)]])))

(defn bench-seek-rd []
  (bench/bench
    (doseq [:let [db   @*db100k]
            id @*ids10k
            :let [from (db/resolve-datom db id :namespaced/full-name nil nil db/e0 db/tx0)
                  to   (db/resolve-datom db id :namespaced/full-name nil nil db/emax db/txmax)]])))

(defn bench-seek-cp []
  (bench/bench
    (doseq [:let [db   @*db100k]
            id @*ids10k
            :let [from (db/components->pattern db :aevt :namespaced/full-name id nil nil db/e0 db/tx0)
                  to   (db/components->pattern db :aevt :namespaced/full-name id nil nil db/emax db/txmax)]])))
            
(defn bench-subslice-aevt []
  (bench/bench
    (doseq [:let [db    @*db100k
                  slice (d/datoms db :aevt :namespaced/full-name)]
            id @*ids10k
            :let [from (db/datom id :namespaced/full-name nil db/tx0)
                  to   (db/datom id :namespaced/full-name nil db/txmax)]]
      (-> (pss/slice slice from to db/cmp-datoms-evt) first :v))))

(defn map-to [ks val-fn]
  (persistent!
    (reduce
      (fn [m k]
        (assoc! m k (val-fn k)))
      (transient {})
      ks)))

#?(:cljs
   (def *roam-db
     (delay
       (let [fun    (gobj/get fs "readFileSync")
             file   (fun "/Users/tonsky/ws/roam/db_3M.json")
             parsed (js/JSON.parse file)]
         (d/from-serializable parsed)))))

#?(:cljs
   (def *roam-ids
     (delay
       (let [index (vec (d/datoms @*roam-db :aevt :block/parents))]
         (->> #(rand-nth index)
           (repeatedly 10000)
           (map :e)
           (into #{}))))))

#?(:cljs
   (defn bench-roam-index []
     (bench/bench
       (let [index (js/Object.)]
         (doseq [d (d/datoms @*roam-db :aevt :block/parents)]
           (gobj/set index (.-e d) (.-v d)))
         (map-to @*roam-ids #(gobj/get index %))))))

#?(:cljs
   (defn bench-roam-eavt []
     (bench/bench
       (map-to @*roam-ids
         #(-> (d/datoms @*roam-db :eavt % :block/parents)
            first
            :v)))))

#?(:cljs
   (defn bench-roam-aevt []
     (bench/bench
       (map-to @*roam-ids
         #(-> (d/datoms @*roam-db :aevt :block/parents %)
            first
            :v)))))

#?(:cljs
   (defn bench-roam-aevt-slice []
     (bench/bench
       (let [slice (d/datoms @*roam-db :aevt :block/parents)]
         (map-to @*roam-ids
           (fn [id]
             (let [from (db/datom id :block/parents nil db/tx0)
                   to   (db/datom id :block/parents nil db/txmax)]
              (some-> (pss/slice slice from to db/cmp-datoms-evt)
                first
                (.-v)))))))))

#?(:cljs
   (defn bench-roam-aevt-slice-cp []
     (bench/bench
       (let [db    @*roam-db
             slice (d/datoms db :aevt :block/parents)]
         (map-to @*roam-ids
           (fn [id]
             (let [from (db/components->pattern db :aevt :block/parents id nil nil db/e0 db/tx0)
                   to   (db/components->pattern db :aevt :block/parents id nil nil db/emax db/txmax)]
              (some-> (pss/slice slice from to db/cmp-datoms-evt)
                first
                (.-v)))))))))

(def *serialize-db 
  (delay
    (d/db-with empty-db
      (take 300000 bench/people))))

#?(:clj
   (def mapper
     (com.fasterxml.jackson.databind.ObjectMapper.)))

(defn bench-freeze []
  (bench/bench
    (-> @*serialize-db (d/serializable) #?(:clj (jsonista/write-value-as-string mapper) :cljs js/JSON.stringify))))

(defn bench-thaw []
  (let [json (-> @*serialize-db (d/serializable) #?(:clj (jsonista/write-value-as-string mapper) :cljs js/JSON.stringify))]
    (bench/bench
      (-> json #?(:clj (jsonista/read-value mapper) :cljs js/JSON.parse) d/from-serializable))))

(def benches
  {"add-1"              bench-add-1
   "add-5"              bench-add-5
   "add-all"            bench-add-all
   "init"               bench-init
   "retract-5"          bench-retract-5
   "q1"                 bench-q1
   "q2"                 bench-q2
   "q3"                 bench-q3
   "q4"                 bench-q4
   "qpred1"             bench-qpred1
   "qpred2"             bench-qpred2
   "pull-one-entities"  bench-pull-one-entities
   "pull-one"           bench-pull-one
   "pull-many-entities" bench-pull-many-entities
   "pull-many"          bench-pull-many
   "pull-wildcard"      bench-pull-wildcard
   "rules-wide-3x3"     bench-rules-wide-3x3
   "rules-wide-5x3"     bench-rules-wide-5x3
   "rules-wide-7x3"     bench-rules-wide-7x3
   "rules-wide-4x6"     bench-rules-wide-4x6
   "rules-long-10x3"    bench-rules-long-10x3
   "rules-long-30x3"    bench-rules-long-30x3
   "rules-long-30x5"    bench-rules-long-30x5
   "seek"               bench-seek
   "seek-cp"            bench-seek-cp
   "seek-rd"            bench-seek-rd
   "slice-eavt"         bench-slice-eavt
   "slice-aevt"         bench-slice-aevt
   "subslice-aevt"      bench-subslice-aevt
   "subslice-aevt-cp"   bench-subslice-aevt-cp
   "freeze"             bench-freeze
   "thaw"               bench-thaw
   #?@(:cljs ["roam-index" bench-roam-index
              "roam-eavt" bench-roam-eavt
              "roam-aevt" bench-roam-aevt
              "roam-aevt-slice" bench-roam-aevt-slice
              "roam-aevt-slice-cp" bench-roam-aevt-slice-cp])})

(defn ^:export -main
  "clj -A:bench -M -m datascript.bench.datascript [--profile] (add-1 | add-5 | ...)*"
  [& args]
  (let [profile? (contains? (set (or args ())) "--profile")
        args     (remove #{"--profile"} args)
        names    (or (not-empty args) (sort (keys benches)))
        _        (apply println #?(:clj "CLJ:" :cljs "CLJS:") names)
        longest  (last (sort-by count names))]
    (binding [bench/*profile* profile?]
      (doseq [name names
              :let [fn (benches name)]]
        (if (nil? fn)
          (println "Unknown benchmark:" name)
          (let [{:keys [mean-ms file]} (fn)]
            (println
              (bench/right-pad name (count longest))
              " "
              (bench/left-pad (bench/round mean-ms) 6) "ms/op"
              " " (or file ""))))))
    #?(:clj (shutdown-agents))))

(comment
  (require 'datascript.bench.datascript :reload-all)

  (bench-add-1)
  (bench-add-5)
  (bench-add-all)
  (bench-init)
  (bench-retract-5)
  (bench-q1)
  (bench-q2)
  (bench-q3)
  (bench-q4)
  (bench-qpred1)
  (bench-qpred2)
  (bench-pull-one-entities)
  (bench-pull-one)
  (bench-pull-many-entities)
  (bench-pull-many)
  (bench-pull-all)
  (binding [bench/*profile* true]
    (bench-pull-one))
  (binding [bench/*profile* true]
    (bench-pull-many))
  (binding [bench/*profile* true]
    (bench-pull-all))
  (bench-rules-wide-3x3)
  (bench-rules-wide-5x3)
  (bench-rules-wide-7x3)
  (bench-rules-wide-4x6)
  (bench-rules-long-10x3)
  (bench-rules-long-30x3)
  (bench-rules-long-30x5)
  (bench-freeze)
  (bench-thaw))
