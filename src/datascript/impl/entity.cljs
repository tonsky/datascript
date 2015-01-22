(ns datascript.impl.entity
  (:require
    [datascript.core :as dc]))

(declare Entity touch)

(defn entity [db eid]
  {:pre [(satisfies? dc/IDB db)
         (satisfies? dc/ISearch db)
         (satisfies? dc/IIndexAccess db)]}
  (when-let [e (dc/entid db eid)]
    (Entity. db e false {})))

(defn- entity-attr [db a datoms]
  (if (dc/multival? db a)
    (if (dc/ref? db a)
      (reduce #(conj %1 (entity db (.-v %2))) #{} datoms)
      (reduce #(conj %1 (.-v %2)) #{} datoms))
    (if (dc/ref? db a)
      (entity db (.-v (first datoms)))
      (.-v (first datoms)))))

(defn- datoms->cache [db datoms]
  (reduce (fn [acc part]
    (let [a (.-a (first part))]
      (assoc acc a (entity-attr db a part))))
    {} (partition-by :a datoms)))

(defn touch-components [db a->v]
  (reduce-kv (fn [acc a v]
               (assoc acc a
                 (if (dc/component? db a)
                   (if (dc/multival? db a)
                     (set (map touch v))
                     (touch v))
                   v)))
             {} a->v))

(defn touch [e]
  (when-not (.-touched e)
    (when-let [datoms (not-empty (dc/-search (.-db e) [(.-eid e)]))]
      (set! (.-touched e) true)
      (set! (.-cache e) (->> datoms
                          (datoms->cache (.-db e))
                          (touch-components (.-db e))))))
  e)

(defn- -lookup-backwards [db eid attr not-found]
  (if-let [datoms (not-empty (dc/-search db [nil attr eid]))]
    (if (dc/component? db attr)
      (entity db (.-e (first datoms)))
      (reduce #(conj %1 (entity db (.-e %2))) #{} datoms)-)
    not-found))

(defn- multival->js [val]
  (when val (to-array val)))

(defn- js-seq [e]
  (touch e)
  (for [[a v] (.-cache e)]
    (if (dc/multival? (.-db e) a)
      [a (multival->js v)]
      [a v])))

(deftype Entity [db eid ^:mutable touched ^:mutable cache]
  Object
  (toString [this]
    (pr-str* this))
  (equiv [this other]
    (-equiv this other))
  
  ;; js/map interface
  (keys [this]
    (es6-iterator (keys this)))
  (entries [this]
    (es6-entries-iterator (js-seq this)))
  (values [this]
    (es6-iterator (map second (js-seq this))))
  (has [this attr]
    (not (nil? (.get this attr))))
  (get [this attr]
    (if (= attr ":db/id")
      eid
      (if (dc/reverse-ref? attr)
        (-> (-lookup-backwards db eid (dc/reverse-ref attr) nil)
            multival->js)
        (cond-> (-lookup this attr)
          (dc/multival? db attr) multival->js))))
  (forEach [this f]
    (doseq [[a v] (js-seq this)]
      (f v a this)))
  (forEach [this f use-as-this]
    (doseq [[a v] (js-seq this)]
      (.call f use-as-this v a this)))
  
  ;; js fallbacks
  (key_set   [this] (to-array (keys this)))
  (entry_set [this] (to-array (map to-array (js-seq this))))
  (value_set [this] (to-array (map second (js-seq this))))

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
      (if (dc/reverse-ref? attr)
        (-lookup-backwards db eid (dc/reverse-ref attr) not-found)
        (or (cache attr)
            (if touched
              not-found
              (if-let [datoms (not-empty (dc/-search db [eid attr]))]
                (do
                  (set! cache (assoc cache attr (entity-attr db attr datoms)))
                  (cache attr))
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


(goog/exportSymbol "datascript.impl.entity.Entity.prototype.get"       (.-get       (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.has"       (.-has       (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.forEach"   (.-forEach   (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.key_set"   (.-key_set   (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.value_set" (.-value_set (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.entry_set" (.-entry_set (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.keys"      (.-keys      (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.values"    (.-values    (.-prototype Entity)))
(goog/exportSymbol "datascript.impl.entity.Entity.prototype.entries"   (.-entries   (.-prototype Entity)))

(goog/exportSymbol "cljs.core.ES6Iterator.prototype.next"        (.-next (.-prototype cljs.core/ES6Iterator)))
(goog/exportSymbol "cljs.core.ES6EntriesIterator.prototype.next" (.-next (.-prototype cljs.core/ES6EntriesIterator)))
