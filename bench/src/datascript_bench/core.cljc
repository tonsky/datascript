(ns datascript-bench.core
  #?(:cljs (:require-macros datascript-bench.core)))

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


(def people (repeatedly random-man))


(def people20k (shuffle (take 20000 people)))


(def ^:dynamic *warmup-t* 5000)
(def ^:dynamic *bench-t*  10000)
(def ^:dynamic *step*     5)
(def ^:dynamic *repeats*  1)


#?(:cljs (defn ^number now [] (js/performance.now))
   :clj  (defn now ^double [] (/ (System/nanoTime) 1000000.0)))


(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
     :clj  (String/format java.util.Locale/ROOT (str "%." places "f") (to-array [(double n)]))))

(defn ^:export round [n]
  (cond
    (> n 1)    (to-fixed n 1)
    (> n 0.01) (to-fixed n 3)
    :else      (to-fixed n 7)))


(defn percentile [xs n]
  (->
    (sort xs)
    (nth (min (dec (count xs))
              (int (* n (count xs)))))))


#?(:clj
  (defmacro dotime [duration & body]
   `(let [start-t# (now)
          end-t#   (+ ~duration start-t#)]
      (loop [iterations# *step*]
        (dotimes [_# *step*] ~@body)
        (let [now# (now)]
          (if (< now# end-t#)
            (recur (+ *step* iterations#))
            (double (/ (- now# start-t#) iterations#)))))))) ;; ms / iteration


#?(:clj
  (defmacro bench [& body]
   `(let [_#       (dotime *warmup-t* ~@body)
          results# (into []
                     (for [_# (range *repeats*)]
                       (dotime *bench-t* ~@body)))
          ; min#     (reduce min results#)
          med#     (percentile results# 0.5)
          ; max#     (reduce max results#)
          ]
      med#)))

