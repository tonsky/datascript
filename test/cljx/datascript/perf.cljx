(ns datascript.perf
  (:require
    [clojure.string :as str]))

(enable-console-print!)

(defn percentile [xs n]
  (-> (sort xs)
    (nth (min (dec (count xs))
              (fix (* n (count xs)))))))

(defn round [x]
  (-> x (* 1000) fix (/ 1000)))

;; (defn now [] (.getTime (js/Date.)))
(defn now [] (.now (.-performance js/window)))

(defn -measure [f {:keys [duration repeats setup-fn] :as opts}]
  (into []
    (for [i (range repeats)
          :let [opts (setup-fn opts)
                t0   (now)]]
        (loop [runs 0]
          (let [dt (- (now) t0)]
          (if (> dt duration)
            (round (/ (* 1000 dt) runs))
            (do
              (f opts)
              (recur (inc runs)))))))))

(defn analyze [results]
  {:unit   "μs"
   :max    (reduce max results)
   :min    (reduce min results)
   :median (percentile results 0.5)
;;    :p90    (percentile results 0.9)
;;    :p99    (percentile results 0.99)
   :raw    results })

(defn measure [msg f opts]
  (println msg)
  (let [results  (-measure f opts)
        analysis (analyze results)]
    (println (select-keys analysis [:unit :min :median :max]) "\n")
    analysis))

(defn params->msg [params]
  (->> (seq params)
       reverse
       (map (fn [[k v]] (str (name k) ": " v)))
       (str/join ", ")))

(defn -suite [f {:keys [matrix params] :as opts}]
  (if (empty? matrix)
    [{ :params (into {} params)
       :result (measure (params->msg params) f opts)}]
    (let [[[key name+values] & rest-matrix] matrix]
      (for [[key-name key-value] (if (map? name+values)
                                   name+values
                                   (map #(vector % %) name+values))
            result (-suite f 
                     (-> opts
                       (assoc :matrix rest-matrix)
                       (assoc key key-value)
                       (update-in [:params] (fnil conj []) [key key-name])))]
        result))))

(defn suite [f & {:as opts}]
  (let [opts (-> (merge {:duration 1000
                         :repeats 5
                         :setup-fn identity}
                        opts)
                 (update-in [:matrix] #(partition 2 %))
                 (update-in [:matrix] reverse))
        results (vec (-suite f opts))]
    (println "SUITE COMPLETED")
    (prn results)
    results))

