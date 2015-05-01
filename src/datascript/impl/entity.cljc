(ns datascript.impl.entity
  (:refer-clojure :exclude [keys get])
  (:require [#? (:cljs cljs.core :clj clojure.core) :as c]
            [datascript.core :as dc]))

(defprotocol TouchableEntity
  (touch [e]))

;; ----------------------------------------------------------------------------

(declare entity ->Entity equiv-entity lookup-entity)

(defn- entity-attr [db a datoms]
  (if (dc/multival? db a)
    (if (dc/ref? db a)
      (reduce #(conj %1 (entity db (.-v %2))) #{} datoms)
      (reduce #(conj %1 (.-v %2)) #{} datoms))
    (if (dc/ref? db a)
      (entity db (.-v (first datoms)))
      (.-v (first datoms)))))

(defn- -lookup-backwards [db eid attr not-found]
  (if-let [datoms (not-empty (dc/-search db [nil attr eid]))]
    (if (dc/component? db attr)
      (entity db (.-e (first datoms)))
      (reduce #(conj %1 (entity db (.-e %2))) #{} datoms))
    not-found))

;;
;; what does it mean to assoc to an entity? it's only necessary
;; for clojure.walk, afaict, but still strange to write.
;; so, we do it, and everything goes into the cache. but it's a bit evil,
;; since it exposes this as API to the user.
;;
(defn- assoc-entity [e k v]
  (let [db  (.-db e)
        eid (.-eid e)]
    (cond
      (= :db k)
      (cond
        (identical? db v) e
        (= db v)          (->Entity v eid (atom @(.-state e)))
        :else             (->Entity v eid (atom {})))

      (#{:db/id :eid} k)
      (cond
        (identical? eid v) e
        (= eid v)          (->Entity db v (atom @(.-state e)))
        :else              (->Entity db v (atom {})))

      :else
      (if (= (c/get (touch e) k) v)
        e
        (swap! (.-state e) update-in [:cache k] v)))))

#?(:cljs
(defn- multival->js [val]
  (when val (to-array val))))

#?(:cljs
(defn- js-seq [e]
  (touch e)
  (for [[a v] (.-cache e)]
    (if (dc/multival? (.-db e) a)
      [a (multival->js v)]
      [a v]))))

#?(
:cljs
(deftype Entity [db eid ^:mutable touched ^:mutable cache]
  Object
  (toString [this]
    (pr-str* this))
  (equiv [this other]
    (equiv-entity this other))

  ;; js/map interface
  (keys [this]
    (es6-iterator (c/keys this)))
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
  (key_set   [this] (to-array (c/keys this)))
  (entry_set [this] (to-array (map to-array (js-seq this))))
  (value_set [this] (to-array (map second (js-seq this))))

  IEquiv
  (-equiv [this o] (equiv-entity this o))

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

:clj ; extend-type can't do Object or IFn, so just split the base type directly
(deftype Entity [db eid state]
  Object
  (toString [e]      (pr-str (assoc (:cache @state) :db/id eid)))
  (hashCode [e]      (hash eid)) ; db?

  clojure.lang.Seqable
  (seq [e]           (touch e) (seq (:cache @state)))

  clojure.lang.Associative
  (equiv [e o]       (equiv-entity e o))
  (containsKey [e k] (lookup-entity e k))
  (entryAt [e k]     (some->> (lookup-entity e k) (vector k)))

  ;; fulfill rest of API for walk; datomic sequence does not return
  ;; the :db/id, but empty does instantiate with that. So we will too.
  (empty [e]         (->Entity (.-db e) (.-eid e) (atom nil)))
  (assoc [e k v]     (assoc-entity e k v))
  (cons  [e [k v]]   (assoc e k v))
  (count [e]         (touch e) (count (.-cache e)))

  clojure.lang.ILookup
  (valAt [e k] (lookup-entity e k))
  (valAt [e k not-found] (lookup-entity e k not-found))
  )
)

(defn entity? [x] (instance? Entity x))

#?(:clj
(defmethod print-method Entity [e, ^java.io.Writer w]
  (.write w (str e))))

(defn- equiv-entity [^Entity this that]
  (and
   (instance? Entity that)
   ;; (= db  (.-db ^Entity that))
   (= (.-eid this) (.-eid ^Entity that))))

#?(:clj
(defn- lookup-entity
  ([this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (let [db (.-db ^Entity this)
         eid (.-eid ^Entity this)]
     (cond
       (= attr :db/id)    eid
       (= attr :db)       db
       (= attr ::cache)   (-> (.-state this) deref :cache)
       (= attr ::touched) (-> (.-state this) deref :touched)
       (dc/reverse-ref? attr) (-lookup-backwards db eid (dc/reverse-ref attr) not-found)
       :else (let [state @(.-state ^Entity this)]
               (or (c/get (:cache state) attr)
                   ;; 'touched' could change between here & swap!, but 'v'
                   ;; below will always be the same, so there's no race --
                   ;; only the rare redundant computation via search,
                   ;; entity-attr, swap!
                   (if (:touched state)
                     not-found
                     (if-let [datoms (not-empty (dc/-search db [eid attr]))]
                       (let [v (entity-attr db attr datoms)]
                         (swap! (.-state ^Entity this) assoc-in [:cache attr] v)
                         v)
                       not-found)))))))))

(defn touch-components [db a->v]
  (reduce-kv (fn [acc a v]
               (assoc acc a
                      (if (dc/component? db a)
                        (if (dc/multival? db a)
                          (set (map touch v))
                          (touch v))
                        v)))
             {} a->v))

(defn- datoms->cache [db datoms]
  (reduce (fn [acc part]
    (let [a (:a (first part))]
      (assoc acc a (entity-attr db a part))))
    {} (partition-by :a datoms)))

(extend-type Entity
  TouchableEntity
  #?(:cljs
      (touch [e]
             (when-not (.-touched e)
               (when-let [datoms (not-empty (dc/-search (.-db e) [(.-eid e)]))]
                 (set! (.-touched e) true)
                 (set! (.-cache e) (->> datoms
                                        (datoms->cache (.-db e))
                                        (touch-components (.-db e))))))
             e)

      :clj
      (touch [^Entity e]
             (let [db (.-db e)
                   eid (.-eid e)]
               (swap! (.-state e)
                      (fn toucher [state]
                        ;; XXX shouldn't touched get updated either way, and cache be left empty if appropriate?
                        (let [{:keys [touched cache]} state]
                          (if-let [datoms (and (not touched) (not-empty (dc/-search db [eid])))]
                            (assoc state
                                   :touched true
                                   :cache (->> datoms (datoms->cache db) (touch-components db)))
                            state)))))
             e) )
  )

(defn entity [db eid]
  {:pre [(satisfies? dc/IDB db)
         (satisfies? dc/ISearch db)
         (satisfies? dc/IIndexAccess db)]}
  (when-let [e (dc/entid db eid)]
    #?(:cljs (->Entity db e false {})
       :clj  (->Entity db e (atom {:touched false, :cache {}})))))

#?(:cljs (do
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
))
