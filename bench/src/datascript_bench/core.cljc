(ns datascript-bench.core
  #?(:cljs (:require-macros datascript-bench.core)))

(def next-eid (volatile! 0))

(defn random-man []
  {:db/id     (str (vswap! next-eid inc))
   :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex       (rand-nth [:male :female])
   :age       (rand-int 100)
   :salary    (rand-int 100000)})


(def people (repeatedly random-man))


(def people20k (shuffle (take 20000 people)))


(def ^:dynamic *warmup-t* 500)
(def ^:dynamic *bench-t*  1000)
(def ^:dynamic *step*     10)
(def ^:dynamic *repeats*  5)


#?(:cljs (defn ^number now [] (js/performance.now))
   :clj  (defn ^Long   now [] (/ (System/nanoTime) 1000000.0)))


(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
     :clj  (format (str "%." places "f") (double n))))


(defn ^:export round [n]
  (cond
    (> n 1)        (to-fixed n 1)
    (> n 0.1)      (to-fixed n 2)
    (> n 0.001)    (to-fixed n 2)
;;     (> n 0.000001) (to-fixed n 7)
    :else          n))


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

