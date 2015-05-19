(ns datascript.debug)

;; ----------------------------------------------------------------------------
;; XXX almost certainly doesn't work in CLJ yet, but compiles.
;;

(def ^:dynamic debug? false)
(def ^:dynamic *minibench-warmups* 50)
(def ^:dynamic *minibench-iterations* 50)
(def enabled? true)

;; ----------------------------------------------------------------------------

(defrecord Frame [parent start end message children])

(def current-frame (volatile! nil))

(defn now [] #?(:cljs (js/window.performance.now) :clj (System/currentTimeMillis)))

(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
     :clj  (format (str "%." places "f") n)))

(defn round [n]
  (cond
    (> n 1)        (to-fixed n 1)
    (> n 0.1)      (to-fixed n 2)
    (> n 0.001)    (to-fixed n 4)
    (> n 0.000001) (to-fixed n 7)
    :else          n))

(defn pad [n l]
  (let [s (str n)]
    (if (<= (count s) l)
      (.slice (str "                  " s) (- l))
      s)))

(defn format-time [dt]
  (str "[ " (pad (round dt) 6) " ms ]"))

(defn emit* [& args]
  #?(:cljs (apply js/console.log args)
     :clj  (apply println args)))

(defn println-frame [frame depth]
  (let [msg (apply str
                   (apply str (repeat depth "  "))
                   (interpose " " (.-message frame)))]
    (if (.-start frame)
      (emit* (format-time (- (.-end frame) (.-start frame))) msg)
      (emit* "[           ]" msg))
    (doseq [ch (.-children frame)]
      (println-frame ch (inc depth)))))

(defn start-frame [time?]
  (vreset! current-frame (Frame. @current-frame (when time? (now)) nil nil [])))

(defn end-frame [& msg]
  (let [f @current-frame
        p (.-parent f)]
    (set! (.-end f) (now))
    (set! (.-message f) msg)
    (vreset! current-frame p)
    (if (nil? p)
      (println-frame f 0)
      (set! (.-children p) (conj (.-children p) f)))))

;; ----------------------------------------------------------------------------

#?(:clj
   (do
     (defmacro do-debug [& body]
       (when enabled?
         `(when debug?
            ~@body)))

     (defmacro debug [& msgs]
       (when enabled?
         `(when debug?
            (start-frame false)
            (end-frame ~@msgs))))

     (defmacro measure [body & msgs]
       (let [sym   (gensym)
             msgs* (clojure.walk/postwalk #(if (= (symbol "%") %) sym %) msgs)]
         (if enabled?
           `(if debug?
              (let [_#   (start-frame true)
                    ~sym ~body]
                (end-frame ~@msgs*)
                ~sym)
              ~body)
           body)))

     (defmacro minibench [msg & body]
       `(let [f#  (fn [] ~@body)
              _#  (dotimes [_# *minibench-warmups*] (f#))
              t#  (now)
              _#  (dotimes [_# *minibench-iterations*] (f#))
              dt# (/ (- (now) t#) *minibench-iterations*)]
          (~`emit* "\n" (format-time dt#) ~msg "avg time")
          (binding [debug? true]
            (f#))))))
