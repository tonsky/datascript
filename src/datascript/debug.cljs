(ns datascript.debug)

(def ^:dynamic debug? false)
(def ^:dynamic *minibench-warmups* 50)
(def ^:dynamic *minibench-iterations* 50)

(defrecord Frame [parent start end message children])

(def current-frame (volatile! nil))

(defn now [] (js/window.performance.now))

(defn round [n]
  (cond
    (> n 1)        (.toFixed n 1)
    (> n 0.1)      (.toFixed n 2)
    (> n 0.001)    (.toFixed n 4)
    (> n 0.000001) (.toFixed n 7)
    :else          n))

(defn pad [n l]
  (let [s (str n)]
    (if (<= (count s) l)
      (.slice (str "                  " s) (- l))
      s)))

(defn format-time [dt]
  (str "[ " (pad (round dt) 6) " ms ]"))

(defn println-frame [frame depth]
  (let [msg (apply str
                   (apply str (repeat depth "  "))
                   (interpose " " (.-message frame)))]
    (if (.-start frame)
      (js/console.log (format-time (- (.-end frame) (.-start frame))) msg)
      (js/console.log "[           ]" msg))
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
