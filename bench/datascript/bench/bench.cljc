(ns datascript.bench.bench
  (:require
   [datascript.core :as d]
   #?(:clj [criterium.core :as criterium])
   #?(:clj [clj-async-profiler.core :as clj-async-profiler]))
  #?(:cljs (:require-macros datascript.bench.bench)))

; Measure time

(def ^:dynamic *warmup-ms* 2000)
(def ^:dynamic *bench-ms*  500)
(def ^:dynamic *samples*   9)
(def ^:dynamic *batch*     100)
(def ^:dynamic *profile*   false)

#?(:cljs (defn ^number now [] (js/performance.now))
   :clj  (defn now ^double [] (/ (System/nanoTime) 1000000.0)))

#?(:clj
   (defmacro dotime
     "Runs form duration, returns average time (ms) per iteration"
     [duration & body]
     `(let [start-t# (now)
            end-t#   (+ ~duration start-t#)]
        (loop [iterations# *batch*]
          (dotimes [_# *batch*] ~@body)
          (let [now# (now)]
            (if (< now# end-t#)
              (recur (+ *batch* iterations#))
              (double (/ (- now# start-t#) iterations#))))))))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

(defn median [xs]
  (nth (sort xs) (quot (count xs) 2)))

(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
     :clj  (String/format java.util.Locale/ROOT (str "%." places "f") (to-array [(double n)]))))

(defn round [n]
  (cond
    (> n 1)    (to-fixed n 1)
    (> n 0.01) (to-fixed n 3)
    :else      (to-fixed n 7)))

(defn left-pad [s l]
  (if (<= (count s) l)
    (str (apply str (repeat (- l (count s)) " ")) s)
    s))

(defn right-pad [s l]
  (if (<= (count s) l)
    (str s (apply str (repeat (- l (count s)) " ")))
    s))

#?(:clj
   (defmacro bench
     "Runs for *wramup-ms* + *bench-ms*, returns median time (ms) per iteration"
     [title & body]
     (let [[title body] (if (string? title)
                         [title body]
                         ["unknown-bench" (cons title body)])]
       (if-cljs &env
         `(let [_#     (dotime *warmup-ms* ~@body)
                times# (mapv
                         (fn [_#]
                           (dotime *bench-ms* ~@body))
                         (range *samples*))]
            {:mean-ms (median times#)})
         `(let [_#      (when *profile* (clj-async-profiler/start {}))
                report# (criterium/quick-benchmark*
                          (fn [] ~@body)
                          (assoc criterium/*default-quick-bench-opts*
                            :samples *samples*
                            :target-execution-time (* *bench-ms* 1000000.0 0.5)
                            :warmup-jit-period (* *warmup-ms* 1000000.0)))
                file#   ^java.io.File (when *profile* (clj-async-profiler/stop {:title ~title}))]
            (cond->
              {:mean-ms (-> report# :mean first (* 1000.0))}
              file# (assoc :file (.getAbsolutePath file#))))))))

;; test dbs

(def next-eid (volatile! 0))

(defn random-man []
  (let [name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
        last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])]
    {:db/id     (str (vswap! next-eid inc))
     :name      name
     :last-name last-name
     :full-name (str name " " last-name)
     :alias     (vec
                  (repeatedly (rand-int 10) #(rand-nth ["A. C. Q. W." "A. J. Finn" "A.A. Fair" "Aapeli" "Aaron Wolfe" "Abigail Van Buren" "Jeanne Phillips" "Abram Tertz" "Abu Nuwas" "Acton Bell" "Adunis"])))
     :sex       (rand-nth [:male :female])
     :age       (rand-int 100)
     :salary    (rand-int 100000)}))

(def schema
  {:follows {:db/valueType   :db.type/ref
             :db/cardinality :db.cardinality/many}
   :alias   {:db/cardinality :db.cardinality/many}})

(def empty-db (d/empty-db schema))

(defn wide-db
  "depth = 3 width = 2

   1
   ├ 2
   │ ├ 4
   │ │ ├ 8
   │ │ └ 9
   │ └ 5
   │   ├ 10
   │   └ 11
   └ 3
     ├ 6
     │ ├ 12
     │ └ 13
     └ 7
       ├ 14
       └ 15"
  ([depth width] (d/db-with (d/empty-db schema) (wide-db 1 depth width)))
  ([id depth width]
    (if (pos? depth)
      (let [children (map #(+ (* id width) %) (range width))]
        (cons
          (assoc (random-man)
            :db/id id
            :follows children)
          (mapcat #(wide-db % (dec depth) width) children)))
      [(assoc (random-man) :db/id id)])))

(defn long-db
  "depth = 3 width = 5

   1  4  7  10  13
   ↓  ↓  ↓  ↓   ↓
   2  5  8  11  14
   ↓  ↓  ↓  ↓   ↓
   3  6  9  12  15"
  [depth width]
  (d/db-with (d/empty-db schema)
    (apply concat
      (for [x (range width)
            y (range depth)
            :let [from (+ (* x (inc depth)) y)
                  to   (+ (* x (inc depth)) y 1)]]
        [{:db/id   from
          :name    "Ivan"
          :follows to}
         {:db/id   to
          :name    "Ivan"}]))))

(def people (repeatedly random-man))

(def *people20k
  (delay
    (shuffle
      (take 20000 people))))

(def *db100k
  (delay
    (d/db-with (d/empty-db schema) @*people20k)))
