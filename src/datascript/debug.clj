(ns datascript.debug)

(def enabled? true)

(defmacro do-debug [& body]
  (when enabled?
   `(when debug?
      ~@body)))

(defmacro debug [& msg]
  (when enabled?
   `(when debug?
      (start-frame false)
      (end-frame ~@msg))))

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
    (js/console.log "\n" (format-time dt#) ~msg "avg time")
    (binding [debug? true]
      (f#))))
