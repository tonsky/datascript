(ns datascript.impl.entity
  (:require
    [datascript.core :as dc]))

(declare Entity)

(defn entity [db eid]
  (Entity. db eid false {}))

(defn touch [e]
  (when-not (.-touched e)
    (when-let [datoms (not-empty (dc/-search (.-db e) [(.-eid e)]))]
      (let [db    (.-db e)
            cache (reduce (fn [acc datom]
                            (let [a (.-a datom)
                                  v (.-v datom)]
                              (if (dc/multival? db a)
                                (assoc! acc a (conj (get acc a #{}) v))
                                (assoc! acc a v))))
                          (transient {}) datoms)]
        (set! (.-touched e) true)
        (set! (.-cache e) (persistent! cache)))))
  e)

(defn- reverse-ref [attr]
  (let [name (name attr)]
    (when (= "_" (nth name 0))
      (keyword (namespace attr) (subs name 1)))))

(deftype Entity [db eid ^:mutable touched ^:mutable cache]
  Object
  (toString [this]
    (pr-str* this))
  (equiv [this other]
    (-equiv this other))

  IEquiv
  (-equiv [_ o]
    (and
      (instance? Entity o)
      ;; (= db  (.-db o))
      (= eid (.-eid o))))
  
  IHash
  (-hash [_]
    (hash eid)) ;; db?
  
  ISeqable
  (-seq [this]
    (touch this)
    (seq cache))
  
  ICounted
  (-count [this]
    (touch this)
    (count cache))
 
  ILookup
  (-lookup [this attr]
    (-lookup this attr nil))
  (-lookup [_ attr not-found]
    (if (= attr :db/id)
      eid
      (if-let [attr (reverse-ref attr)]
        (if-let [datoms (not-empty (dc/-search db [nil attr eid]))]
          (reduce #(conj %1 (entity db (.-e %2))) #{} datoms)
          not-found)
        (or (cache attr)
            (if touched
              not-found
              (if-let [datoms (not-empty (dc/-search db [eid attr]))]
                (let [wrap (if (dc/ref? db attr) #(entity db (.-v %)) #(.-v %))
                      val  (if (dc/multival? db attr)
                             (reduce #(conj %1 (wrap %2)) #{} datoms)
                             (wrap (first datoms)))]
                  (set! cache (assoc cache attr val))
                  val)
                not-found))))))

  IAssociative
  (-contains-key? [this k]
    (not= ::nf (-lookup this k ::nf)))

  IFn
  (-invoke [this k]
    (-lookup this k))
  (-invoke [this k not-found]
    (-lookup this k not-found))

  IPrintWithWriter
  (-pr-writer [_ writer opts]
    (-pr-writer (assoc cache :db/id eid) writer opts)))
  
