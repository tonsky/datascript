(ns datascript.bench-datomic
  (:require
    [clojure.string :as str]
    [datomic.api :as d]
    [datomic.btset :as btset]
    [datascript.bench :as bench]
    [datascript.perf :as perf]))

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

(defn new-conn []
  (d/delete-database "datomic:mem://bench")
  (d/create-database "datomic:mem://bench")
  (let [conn (d/connect "datomic:mem://bench")]
    @(d/transact conn
      [ (schema-attr :name      :db.type/string)
        (schema-attr :last-name :db.type/string)
        (schema-attr :sex       :db.type/keyword)
        (schema-attr :age       :db.type/long)
        (schema-attr :salary    :db.type/long)
        (schema-attr :follows   :db.type/ref, :db/cardinality :db.cardinality/many)])
    conn))

(defn db-with [tx-data]
  (-> (new-conn)
      (d/transact tx-data)
      deref
      :db-after))

(defn tempid [i]
  (d/tempid :db.part/user (- i)))

(defn- wide-db
  ([depth width] (db-with (wide-db 1 depth width)))
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

(def people (map #(assoc % :db/id (d/tempid :db.part/user)) bench/people))

(defn bench-db_with []
  (doseq [size  [100 500 2000]
          batch [1 10 100]
          :let [parts (->> (take size people)
                           (partition-all batch))]]
    (perf/bench {:test "db_with" :size size :batch batch}
      (let [conn (new-conn)]
        (doseq [people parts]
          @(d/transact conn people))))))

(defn bench-queries []
  (doseq [[n q] [["q1" '[:find ?e       :where [?e :name "Ivan"]]]
                 ["q2" '[:find ?e ?a    :where [?e :name "Ivan"]
                                               [?e :age ?a]]]
                 ["q3" '[:find ?e ?a    :where [?e :name "Ivan"]
                                               [?e :age ?a]
                                               [?e :sex :male]]]
                 ["q4" '[:find ?e ?l ?a :where [?e :name "Ivan"]
                                               [?e :last-name ?l]
                                               [?e :age ?a]
                                               [?e :sex :male]]]]
          size [100 500 2000 20000]
          :let [db (db-with (take size people))]]
    (perf/bench {:test n :size size}
      (d/q q db))))
    
(defn bench-rules []
  (doseq [[id db] [["wide 3×3"  (wide-db 3 3)]
                   ["wide 5×3"  (wide-db 5 3)]
                   ["wide 7×3"  (wide-db 7 3)]
                   ["wide 4×6"  (wide-db 4 6)]
                   ["long 10×3" (long-db 10 3)]
                   ["long 30×3" (long-db 30 3)]
                   ["long 30×5" (long-db 30 5)]]]
    (perf/bench {:test "rules" :form id}
      (d/q '[:find ?e ?e2
             :in   $ %
             :where (follows ?e ?e2)]
           db
           '[[(follows ?x ?y)
              [?x :follows ?y]]
             [(follows ?x ?y)
              [?x :follows ?t]
              (follows ?t ?y)]]))))

(defn bench-btset []
  (doseq [[tn target] [;;["sorted-set" (sorted-set)]
                       ;;["btset"      (btset/btset)]
                       ["datomic/btset" (btset/btset)]]
;;           distinct?   [true false]
          size        [100 500 20000]
          :let [range          (if true ;; distinct?
                                 (shuffle (range size))
                                 (repeatedly size #(rand-int size)))
                shuffled-range (shuffle range)
                set            (into target range)]]
    (perf/bench {:target tn :test "set-conj" :size size}
      (into target range))
;;     (perf/bench {:target tn :test "set-disj" :size size}
;;       (reduce disj set shuffled-range))
    (perf/bench {:target tn :test "set-lookup" :size size}
      (doseq [i shuffled-range]
        (contains? set i)))
    (perf/bench {:target tn :test "set-iterate" :size size}
      (doseq [x set]
        (+ 1 x)))))

(defn bench-all []
  (bench-db_with)
  (bench-queries)
  (bench-rules)
  (bench-btset))
