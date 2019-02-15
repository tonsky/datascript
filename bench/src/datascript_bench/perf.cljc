(ns datascript-bench.perf
  (:require
    [clojure.string :as str]
    [datascript-bench.core :as core]
    #?(:clj clojure.java.shell))
  #?(:cljs (:require-macros datascript-bench.perf)))

(def ^:const   enabled? true)
(def ^:dynamic debug?   false)

#?(:cljs (enable-console-print!))

;; helpers

(defn pad [n l]
  (let [s (str n)]
    (if (<= (count s) l)
      (str (apply str (repeat (- l (count s)) " ")) s)
      s)))

(defn format-number [n]
  (pad (core/round n) 6))

(defn format-time [dt]
  (str "[ " (format-number dt) " ms ]"))

(defn inst []
#?(:cljs (js/Date.)
   :clj  (java.util.Date.)))


;; flame graph

(defrecord Frame [parent start end message children])

(def current-frame (volatile! nil))

(defn println-frame [frame depth]
  (let [msg (apply str
                   (apply str (repeat depth "  "))
                   (interpose " " (:message frame)))]
    (if (:start frame)
      (println (format-time (- (:end frame) (:start frame))) msg)
      (println "[           ]" msg))
    (doseq [ch (:children frame)]
      (println-frame ch (inc depth)))))

(defn start-frame [time?]
  (vreset! current-frame (->Frame @current-frame (when time? (core/now)) nil nil [])))

(defn end-frame [& msg]
  (let [f ^Frame @current-frame
        f (assoc f
            :end (core/now)
            :message msg)
        p (some-> (:parent f)
                  (update :children conj f))]
    (when (nil? p)
      (println-frame f 0))
    (vreset! current-frame p)))

#?(:clj
  (defmacro when-debug [& body]
    (when enabled?
      `(when debug?
         ~@body))))

#?(:clj
  (defmacro debug [& msgs]
    (when enabled?
      `(when debug?
         (start-frame false)
         (end-frame ~@msgs)))))

#?(:clj
  (defmacro measure [body & msgs]
    (if enabled?
      (let [sym   (gensym)
            msgs* (clojure.walk/postwalk #(if (= (symbol "%") %) sym %) msgs)]
       `(if debug?
          (let [_#   (start-frame true)
                ~sym ~body]
            (end-frame ~@msgs*)
            ~sym)
          ~body))
      body)))


(defn short-circuit-frames [& [msg]]
  (loop []
    (when-not (nil? @current-frame)
      (end-frame (or msg "↑↑↑ exception ↑↑↑"))
      (recur))))


#?(:clj
   (defmacro with-debug [& body]
    `(binding [debug? true]
       (try
         ~@body
         (finally
           (short-circuit-frames))))))

;; minibench

; #?(:clj
;   (defmacro minibench [spec & body]
;    `(let [_#     (dotime *warmup-t* ~@body)
;           avg-t# (dotime *bench-t* ~@body)]
;       (println (format-time avg-t#) ~spec "avg time")
;       (with-debug
;         ~@body))))
