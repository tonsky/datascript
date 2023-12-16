(ns datascript.util
  #?(:clj
     (:import
       [java.util UUID])))

(def ^:dynamic *debug*
  false)

(defmacro log [& body]
  `(when *debug*
     (println ~@body)))

(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

#?(:cljs
   (defn- to-hex-string [n l]
     (let [s (.toString n 16)
           c (count s)]
       (cond
         (> c l) (subs s 0 l)
         (< c l) (str (apply str (repeat (- l c) "0")) s)
         :else   s))))

(defn squuid
  ([]
   (squuid #?(:clj  (System/currentTimeMillis)
              :cljs (.getTime (js/Date.)))))
  ([msec]
   #?(:clj
      (let [uuid     (UUID/randomUUID)
            time     (int (/ msec 1000))
            high     (.getMostSignificantBits uuid)
            low      (.getLeastSignificantBits uuid)
            new-high (bit-or (bit-and high 0x00000000FFFFFFFF)
                       (bit-shift-left time 32)) ]
        (UUID. new-high low))
      :cljs
      (uuid
        (str
          (-> (int (/ msec 1000))
            (to-hex-string 8))
          "-" (-> (rand-bits 16) (to-hex-string 4))
          "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
          "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
          "-" (-> (rand-bits 16) (to-hex-string 4))
          (-> (rand-bits 16) (to-hex-string 4))
          (-> (rand-bits 16) (to-hex-string 4)))))))

(defn squuid-time-millis
  "Returns time that was used in [[squuid]] call, in milliseconds, rounded to the closest second."
  [uuid]
  #?(:clj (-> (.getMostSignificantBits ^UUID uuid)
            (bit-shift-right 32)
            (* 1000))
     :cljs (-> (subs (str uuid) 0 8)
             (js/parseInt 16)
             (* 1000))))

(defn distinct-by [f coll]
  (->>
    (reduce
      (fn [[seen res :as acc] el]
        (let [key (f el)]
          (if (contains? seen key)
            acc
            [(conj! seen key) (conj! res el)])))
      [(transient #{}) (transient [])]
      coll)
    second
    persistent!))
