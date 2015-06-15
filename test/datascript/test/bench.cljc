(ns datascript.test.bench
  (:require
    [clojure.string :as str]
    [datascript :as d]
    [datascript.btset :as btset]
    [datascript.core :as dc #?@(:cljs [:refer [DB]])]
    [datascript.parser :as dp]
    [datascript.perf :as perf])
  #?(:clj (:import [datascript.core DB])))

#?(:cljs
   (enable-console-print!))

;; test-db

(def next-eid (volatile! 0))

(defn random-man []
  (with-meta
    {:db/id     (vswap! next-eid inc)
     :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
     :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
     :sex       (rand-nth [:male :female])
     :age       (rand-int 10)
     :salary    (rand-int 100000)}
    {:tx (+ d/tx0 (rand-int 1000))}))

(def people (repeatedly random-man))

(def xf-ent->datom
  (mapcat (fn [p]
            (reduce-kv
              (fn [acc k v]
                (conj acc (d/datom (:db/id p) k v (:tx (meta p)))))
              [] (dissoc p :db/id)))))

(defn- wide-db
  ([depth width] (d/db-with (d/empty-db) (wide-db 1 depth width)))
  ([id depth width]
    (if (pos? depth)
      (let [children (map #(+ (* id width) %) (range width))]
        (concat
          (map #(vector :db/add id :follows %) children)
          (mapcat #(wide-db % (dec depth) width) children)))
      [])))

(defn- long-db [depth width]
  (d/db-with (d/empty-db)
    (for [x (range width)
          y (range depth)]
      [:db/add (+ (* x (inc depth)) y) :follows (+ (* x (inc depth)) y 1)])))

;; tests

(defn ^:export bench-transact []
  (doseq [size  [100 500 2000]
          batch [1]
          :let [part (->> (take size people)
                          (partition-all batch))]]
    (perf/bench {:test "db-with" :size size :batch batch}
      (reduce d/db-with (d/empty-db) part))))

(defn ^:export bench-init-db []
  (doseq [size [100 500 2000 5000 20000]
          :let [datoms (into [] (comp xf-ent->datom (take size)) people)]]
    (perf/bench {:test "init-db" :size size}
      (d/init-db datoms))))

(defn ^:export bench-queries []
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
          :let [db (d/db-with (d/empty-db) (take size people))]]
    (perf/bench {:test n :size size}
      (d/q q db))))
    
(defn ^:export bench-rules []
  (doseq [[id db] [["wide 3×3" (wide-db 3 3)]
                   ["wide 5×3" (wide-db 5 3)]
                   ["wide 7×3" (wide-db 7 3)]
                   ["wide 4×6" (wide-db 4 6)]
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

(defn ^:export bench-btset []
  (doseq [[tn target] [["sorted-set" (sorted-set)]
                       ["btset"      (btset/btset)]]
;;           distinct?   [true false]
          size        [100 500 20000]
          :let [range          (if true ;; distinct?
                                 (shuffle (range size))
                                 (repeatedly size #(rand-int size)))
                shuffled-range (shuffle range)
                set            (into target range)]]
    (perf/bench {:target tn :test "set-conj" :size size}
      (into target range))
    (perf/bench {:target tn :test "set-disj" :size size}
      (reduce disj set shuffled-range))
    (perf/bench {:target tn :test "set-lookup" :size size}
      (doseq [i shuffled-range]
        (contains? set i)))
    (perf/bench {:target tn :test "set-iterate" :size size}
      (doseq [x set]
        (+ 1 x)))))

(defn ^:export bench-all []
  (perf/set-context!
    { :project (perf/cljs? "datascript/v8" "datascript/jvm")
      :build   (str (perf/git-commit-count)
                    "/" (subs (perf/git-commit-sha1) 0 7)
                    "/" (str/replace (perf/git-commit-descr) #"-(\d+)-g.+" "+$1")) })
  (bench-transact)
  (bench-init-db)
  (bench-queries)
  (bench-rules)
  (bench-btset))
