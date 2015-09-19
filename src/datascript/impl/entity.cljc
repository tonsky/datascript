(ns datascript.impl.entity
  (:refer-clojure :exclude [keys get])
  (:require [#?(:cljs cljs.core :clj clojure.core) :as c]
            [datascript.db :as db]))

(declare entity ->Entity equiv-entity lookup-entity touch)

(defn- entid [db eid]
  (when (or (number? eid)
            (sequential? eid))
    (db/entid db eid)))

(defn entity [db eid]
  {:pre [(db/db? db)]}
  (when-let [e (entid db eid)]
    (->Entity db e (volatile! false) (volatile! {}))))

(defn- entity-attr [db a datoms]
  (if (db/multival? db a)
    (if (db/ref? db a)
      (reduce #(conj %1 (entity db (:v %2))) #{} datoms)
      (reduce #(conj %1 (:v %2)) #{} datoms))
    (if (db/ref? db a)
      (entity db (:v (first datoms)))
      (:v (first datoms)))))

(defn- -lookup-backwards [db eid attr not-found]
  (if-let [datoms (not-empty (db/-search db [nil attr eid]))]
    (if (db/component? db attr)
      (entity db (:e (first datoms)))
      (reduce #(conj %1 (entity db (:e %2))) #{} datoms))
    not-found))

#?(:cljs
   (defn- multival->js [val]
     (when val (to-array val))))

#?(:cljs
   (defn- js-seq [e]
     (touch e)
     (for [[a v] @(.-cache e)]
       (if (db/multival? (.-db e) a)
         [a (multival->js v)]
         [a v]))))

(deftype Entity [db eid touched cache]
  #?@(:cljs
      [Object
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
              (if (db/reverse-ref? attr)
                (-> (-lookup-backwards db eid (db/reverse-ref attr) nil)
                    multival->js)
                (cond-> (lookup-entity this attr)
                  (db/multival? db attr) multival->js))))
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
             (seq @cache))

       ICounted
       (-count [this]
               (touch this)
               (count @cache))

       ILookup
       (-lookup [this attr]           (lookup-entity this attr nil))
       (-lookup [this attr not-found] (lookup-entity this attr not-found))

       IAssociative
       (-contains-key? [this k]
                       (not= ::nf (lookup-entity this k ::nf)))

       IFn
       (-invoke [this k]
                (lookup-entity this k))
       (-invoke [this k not-found]
                (lookup-entity this k not-found))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-pr-writer (assoc @cache :db/id eid) writer opts))]

      :clj
      [Object
       (toString [e]      (pr-str (assoc @cache :db/id eid)))
       (hashCode [e]      (hash eid)) ; db?

       clojure.lang.Seqable
       (seq [e]           (touch e) (seq @cache))

       clojure.lang.Associative
       (equiv [e o]       (equiv-entity e o))
       (containsKey [e k] (not= ::nf (lookup-entity e k ::nf)))
       (entryAt [e k]     (some->> (lookup-entity e k) (clojure.lang.MapEntry. k)))

       (empty [e]         (throw (UnsupportedOperationException.)))
       (assoc [e k v]     (throw (UnsupportedOperationException.)))
       (cons  [e [k v]]   (throw (UnsupportedOperationException.)))
       (count [e]         (touch e) (count @(.-cache e)))

       clojure.lang.ILookup
       (valAt [e k]       (lookup-entity e k))
       (valAt [e k not-found] (lookup-entity e k not-found))

       clojure.lang.IFn
       (invoke [e k]      (lookup-entity e k))
       (invoke [e k not-found] (lookup-entity e k not-found))
       ]))

(defn entity? [x] (instance? Entity x))

#?(:clj
   (defmethod print-method Entity [e, ^java.io.Writer w]
     (.write w (str e))))

(defn- equiv-entity [^Entity this that]
  (and
   (instance? Entity that)
   ;; (= db  (.-db ^Entity that))
   (= (.-eid this) (.-eid ^Entity that))))

(defn- lookup-entity
  ([this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (if (= attr :db/id)
     (.-eid this)
     (if (db/reverse-ref? attr)
       (-lookup-backwards (.-db this) (.-eid this) (db/reverse-ref attr) not-found)
       (or (@(.-cache this) attr)
           (if @(.-touched this)
             not-found
             (if-let [datoms (not-empty (db/-search (.-db this) [(.-eid this) attr]))]
               (let [value (entity-attr (.-db this) attr datoms)]
                 (vreset! (.-cache this) (assoc @(.-cache this) attr value))
                 value)
               not-found)))))))

(defn touch-components [db a->v]
  (reduce-kv (fn [acc a v]
               (assoc acc a
                 (if (db/component? db a)
                   (if (db/multival? db a)
                     (set (map touch v))
                     (touch v))
                   v)))
             {} a->v))

(defn- datoms->cache [db datoms]
  (reduce (fn [acc part]
    (let [a (:a (first part))]
      (assoc acc a (entity-attr db a part))))
    {} (partition-by :a datoms)))

(defn touch [^Entity e]
  {:pre [(entity? e)]}
  (when-not @(.-touched e)
    (when-let [datoms (not-empty (db/-search (.-db e) [(.-eid e)]))]
      (vreset! (.-cache e) (->> datoms
                                (datoms->cache (.-db e))
                                (touch-components (.-db e))))
      (vreset! (.-touched e) true)))
  e)

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
