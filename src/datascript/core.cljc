(ns datascript.core
  #?(:cljs (:refer-clojure :exclude [array? seqable? alength]))
  #?(:cljs (:require-macros [datascript.core :refer [case-tree combine-cmp raise defrecord-updatable]]))
  (:require
   #?@(:cljs [[cljs.core :as c]
              [goog.array :as garray]]
       :clj  [[clojure.core :as c]])
    clojure.walk
   [datascript.btset :as btset]))

;; ----------------------------------------------------------------------------

#?(:cljs (defn arr [size](make-array size))
   :clj  (defn ^{:tag "[[Ljava.lang.Object;"} arr [size]
           (make-array java.lang.Object size)))

(defn into-arr [aseq]
  #?(:cljs (into-array aseq)
     :clj  (into-array java.lang.Object aseq)))

(def array?
  #?(:cljs c/array?
     :clj  (fn array? [^Object x] (-> x .getClass .isArray))))

(def seqable?
  #?(:cljs c/seqable?
     :clj (fn seqable? [x]
            (or (seq? x)
                (instance? clojure.lang.Seqable x)
                (nil? x)
                (instance? Iterable x)
                (array? x)
                (string? x)
                (instance? java.util.Map x)))))

(def neg-number? (every-pred number? neg?))

#?(:cljs
   (do
     (def Exception js/Error)
     (def IllegalArgumentException js/Error)
     (def UnsupportedOperationException js/Error)))

(def ^:const tx0 0x20000000)
(def ^:const default-schema nil)

;; ----------------------------------------------------------------------------

#?(:clj
  (defmacro raise [& fragments]
    (let [msgs (butlast fragments)
          data (last fragments)]
      `(throw (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)))))

;; ----------------------------------------------------------------------------
;; macros and funcs to support writing defrecords and updating
;; (replacing) builtins, i.e., Object/hashCode, IHashEq hasheq, etc.
;; code taken from prismatic:
;;  https://github.com/Prismatic/schema/commit/e31c419c56555c83ef9ee834801e13ef3c112597
;;

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

#?(:clj
   (defn- get-sig [method]
     ;; expects something like '(method-symbol [arg arg arg] ...)
     ;; if the thing matches, returns [fully-qualified-symbol arity], otherwise nil
     (and (sequential? method)
          (symbol? (first method))
          (vector? (second method))
          (let [sym (first method)
                ns  (or (some->> sym resolve meta :ns str) "clojure.core")]
            [(symbol ns (name sym)) (-> method second count)]))))

#?(:clj
   (defn- dedupe-interfaces [deftype-form]
     ;; get the interfaces list, remove any duplicates, similar to remove-nil-implements in potemkin
     ;; verified w/ deftype impl in compiler:
     ;; (deftype* tagname classname [fields] :implements [interfaces] :tag tagname methods*)
     (let [[deftype* tagname classname fields implements interfaces & rest] deftype-form]
       (when (or (not= deftype* 'deftype*) (not= implements :implements))
         (throw (IllegalArgumentException. "deftype-form mismatch")))
       (list* deftype* tagname classname fields implements (vec (distinct interfaces)) rest))))

#?(:clj
   (defn- make-record-updatable-clj [name fields & impls]
     (let [impl-map (->> impls (map (juxt get-sig identity)) (filter first) (into {}))
           body     (macroexpand-1 (list* 'defrecord name fields impls))]
       (clojure.walk/postwalk
        (fn [form]
          (if (and (sequential? form) (= 'deftype* (first form)))
            (->> form
                 dedupe-interfaces
                 (remove (fn [method]
                           (when-let [impl (-> method get-sig impl-map)]
                             (not= method impl)))))
            form))
        body))))

#?(:clj
   (defn- make-record-updatable-cljs [name fields & impls]
     `(do
        (defrecord ~name ~fields)
        (extend-type ~name ~@impls))))

#?(:clj
   (defmacro defrecord-updatable [name fields & impls]
     `(if-cljs
       ~(apply make-record-updatable-cljs name fields impls)
       ~(apply make-record-updatable-clj  name fields impls))))

;; ----------------------------------------------------------------------------

(declare hash-datom equiv-datom seq-datom val-at-datom assoc-datom)

(deftype Datom [e a v tx added]
  #?@(:cljs
       [IHash
        (-hash [d] (or (.-__hash d)
                       (set! (.-__hash d) (hash-datom d))))
        IEquiv
        (-equiv [d o] (and (instance? Datom o) (equiv-datom d o)))

        ISeqable
        (-seq [d] (seq-datom d))

        ILookup
        (-lookup [d k] (val-at-datom d k nil))
        (-lookup [d k nf] (val-at-datom d k nf))

        IAssociative
        (-assoc [d k v] (assoc-datom k))

        IPrintWithWriter
        (-pr-writer [d writer opts]
                    (pr-sequential-writer writer pr-writer
                                          "#datascript/Datom [" " " "]"
                                          opts [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)]))
        ]
       :clj
       [Object
        (hashCode [d] (hash-datom d))

        clojure.lang.IHashEq
        (hasheq [d] (hash-datom d))

        clojure.lang.Seqable
        (seq [d] (seq-datom d))

        clojure.lang.IPersistentCollection
        (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
        (empty [d] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
        (count [d] 5)
        (cons [d [k v]] (assoc-datom k v))

        clojure.lang.ILookup
        (valAt [d k] (val-at-datom d k nil))
        (valAt [d k nf] (val-at-datom d k nf))

        clojure.lang.Associative
        (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry k)))
        (containsKey [e k] (#{:e :a :v :tx :added} k))
        (assoc [d k v] (assoc-datom k v))
        ]))

(defn ^Datom datom
  ([e a v]          (Datom. e a v tx0 true))
  ([e a v tx]       (Datom. e a v tx true))
  ([e a v tx added] (Datom. e a v tx added)))
  
(defn datom? [x] (instance? Datom x))

(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (hash-combine (hash (.-a d)))
      (hash-combine (hash (.-v d)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (= (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (= (.-v d) (.-v o))))

(defn- seq-datom [^Datom d]
  (list (.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)))

;; keep it fast by duplicating for both keyword and string cases
;; instead of using sets or some other matching func
(defn- val-at-datom [^Datom d k not-found]
  (case k
    :e     (.-e d)        "e"     (.-e d)
    :a     (.-a d)        "a"     (.-a d)
    :v     (.-v d)        "v"     (.-v d)
    :tx    (.-tx d)       "tx"    (.-tx d)
    :added (.-added d)    "added" (.-added d)
    not-found))

(defn- ^Datom assoc-datom [^Datom d k v]
  (case k
    :e     (Datom. v       (.-a d) (.-v d) (.-tx d) (.-added d))
    :a     (Datom. (.-e d) v       (.-v d) (.-tx d) (.-added d))
    :v     (Datom. (.-e d) (.-a d) v       (.-tx d) (.-added d))
    :tx    (Datom. (.-e d) (.-a d) (.-v d) v        (.-added d))
    :added (Datom. (.-e d) (.-a d) (.-v d) (.-tx d) v)
    (throw (IllegalArgumentException. (str "invalid key for #datascript/Datom: " k)))))

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(defn ^Datom datom-from-reader [vec]
  (apply datom vec))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#datascript/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)]))))

;; ----------------------------------------------------------------------------
;; datom cmp macros/funcs
;;

#?(:clj
  (defmacro combine-cmp [& comps]
    (loop [comps (reverse comps)
           res   (num 0)]
      (if (not-empty comps)
        (recur
          (next comps)
          `(let [c# ~(first comps)]
             (if (== 0 c#)
               ~res
               c#)))
        res))))

#?(:clj
   (defn- -case-tree [queries variants]
     (if queries
       (let [v1 (take (/ (count variants) 2) variants)
             v2 (drop (/ (count variants) 2) variants)]
         (list 'if (first queries)
               (-case-tree (next queries) v1)
               (-case-tree (next queries) v2)))
       (first variants))))

#?(:clj
   (defmacro case-tree [qs vs]
     (-case-tree qs vs)))

(defn- cmp [o1 o2]
  (if (and o1 o2)
    (compare o1 o2)
    0))

(defn- cmp-num [n1 n2]
  (if (and n1 n2)
    (- n1 n2)
    0))

(defn cmp-val [o1 o2]
  (if (and (some? o1) (some? o2))
    (let [t1 (type o1)
          t2 (type o2)]
      (if (identical? t1 t2)
        (compare o1 o2)
        #?(:cljs (garray/defaultCompare t1 t2)
           :clj  (compare (.getName ^Class t1) (.getName ^Class t2)))))
    0))

;; Slower cmp-* fns allows for datom fields to be nil.
;; Such datoms come from slice method where they are used as boundary markers.

(defn cmp-datoms-eavt [^Datom d1, ^Datom d2]
  (combine-cmp
    (cmp-num (.-e d1) (.-e d2))
    (cmp (.-a d1) (.-a d2))
    (cmp-val (.-v d1) (.-v d2))
    (cmp-num (.-tx d1) (.-tx d2))))

(defn cmp-datoms-aevt [^Datom d1, ^Datom d2]
  (combine-cmp
    (cmp (.-a d1) (.-a d2))
    (cmp-num (.-e d1) (.-e d2))
    (cmp-val (.-v d1) (.-v d2))
    (cmp-num (.-tx d1) (.-tx d2))))

(defn cmp-datoms-avet [^Datom d1, ^Datom d2]
  (combine-cmp
    (cmp (.-a d1) (.-a d2))
    (cmp-val (.-v d1) (.-v d2))
    (cmp-num (.-e d1) (.-e d2))
    (cmp-num (.-tx d1) (.-tx d2))))


;; fast versions without nil checks

(defn- cmp-attr-quick [a1 a2]
  ;; either both are keywords or both are strings
  #?(:cljs
     (if (keyword? a1)
       (-compare a1 a2)
       (garray/defaultCompare a1 a2))
     :clj
     (compare a1 a2)))

(defn- cmp-val-quick [o1 o2]
  (let [t1 (type o1)
        t2 (type o2)]
    #?(:cljs
       (if (identical? t1 t2)
         (compare o1 o2)
         (garray/defaultCompare t1 t2))
       :clj
       (combine-cmp
        (compare (str t1) (str t2))
        (compare o1 o2)))))

(defn cmp-datoms-eavt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
    (- (.-e d1) (.-e d2))
    (cmp-attr-quick (.-a d1) (.-a d2))
    (cmp-val-quick  (.-v d1) (.-v d2))
    (- (.-tx d1) (.-tx d2))))

(defn cmp-datoms-aevt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
    (cmp-attr-quick (.-a d1) (.-a d2))
    (- (.-e d1) (.-e d2))
    (cmp-val-quick (.-v d1) (.-v d2))
    (- (.-tx d1) (.-tx d2))))

(defn cmp-datoms-avet-quick [^Datom d1, ^Datom d2]
  (combine-cmp
    (cmp-attr-quick (.-a d1) (.-a d2))
    (cmp-val-quick  (.-v d1) (.-v d2))
    (- (.-e d1) (.-e d2))
    (- (.-tx d1) (.-tx d2))))

;; ----------------------------------------------------------------------------

;;;;;;;;;; Searching

(defprotocol ISearch
  (-search [data pattern]))

(defprotocol IIndexAccess
  (-datoms [db index components])
  (-seek-datoms [db index components])
  (-index-range [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-attrs-by [db property]))

;; ----------------------------------------------------------------------------

(declare hash-db hash-fdb equiv-db empty-db pr-db resolve-datom validate-attr components->pattern)

(defrecord-updatable DB [schema eavt aevt avet max-eid max-tx rschema #?(:clj __hash)]
  #?@(:cljs
      [IHash                (-hash  [db]        (hash-db db))
       IEquiv               (-equiv [db other]  (equiv-db db other))
       ISeqable             (-seq   [db]        (-seq  (.-eavt db)))
       IReversible          (-rseq  [db]        (-rseq (.-eavt db)))
       ICounted             (-count [db]        (count (.-eavt db)))
       IEmptyableCollection (-empty [db]        (empty-db (.-schema db)))
       IPrintWithWriter     (-pr-writer [db w opts] (pr-db db w opts))]

      :clj
      [Object               (hashCode [db]      (hash-db db))
       clojure.lang.IHashEq (hasheq [db]        (hash-db db))
       clojure.lang.Seqable (seq [db]           (seq eavt))
       clojure.lang.IPersistentCollection
                            (count [db]         (count eavt))
                            (equiv [db other]   (equiv-db db other))
                            (empty [db]         (empty-db schema))])

  IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property))

  ISearch
  (-search [db pattern]
           (let [[e a v tx] pattern
                 eavt (.-eavt db)
                 aevt (.-aevt db)
                 avet (.-avet db)]
             (case-tree [e a (some? v) tx]
                        [(btset/slice eavt (Datom. e a v tx nil))              ;; e a v tx
                         (btset/slice eavt (Datom. e a v nil nil))             ;; e a v _
                         (->> (btset/slice eavt (Datom. e a nil nil nil))      ;; e a _ tx
                              (filter (fn [^Datom d] (= tx (.-tx d)))))
                         (btset/slice eavt (Datom. e a nil nil nil))           ;; e a _ _
                         (->> (btset/slice eavt (Datom. e nil nil nil nil))    ;; e _ v tx
                              (filter (fn [^Datom d] (and (= v (.-v d))
                                                          (= tx (.-tx d))))))
                         (->> (btset/slice eavt (Datom. e nil nil nil nil))    ;; e _ v _
                              (filter (fn [^Datom d] (= v (.-v d)))))
                         (->> (btset/slice eavt (Datom. e nil nil nil nil))    ;; e _ _ tx
                              (filter (fn [^Datom d] (= tx (.-tx d)))))
                         (btset/slice eavt (Datom. e nil nil nil nil))         ;; e _ _ _
                         (->> (btset/slice avet (Datom. nil a v nil nil))      ;; _ a v tx
                              (filter (fn [^Datom d] (= tx (.-tx d)))))
                         (btset/slice avet (Datom. nil a v nil nil))           ;; _ a v _
                         (->> (btset/slice avet (Datom. nil a nil nil nil))    ;; _ a _ tx
                              (filter (fn [^Datom d] (= tx (.-tx d)))))
                         (btset/slice avet (Datom. nil a nil nil nil))         ;; _ a _ _
                         (filter (fn [^Datom d] (and (= v (.-v d))
                                                     (= tx (.-tx d)))) eavt)   ;; _ _ v tx
                         (filter (fn [^Datom d] (= v (.-v d))) eavt)           ;; _ _ v _
                         (filter (fn [^Datom d] (= tx (.-tx d))) eavt)         ;; _ _ _ tx
                         eavt])))                                              ;; _ _ _ _

  IIndexAccess
  (-datoms [db index cs]
           (btset/slice (get db index) (components->pattern db index cs)))

  (-seek-datoms [db index cs]
                (btset/slice (get db index) (components->pattern db index cs) (Datom. nil nil nil nil nil)))

  (-index-range [db attr start end]
                (validate-attr attr (list '-index-range 'db attr start end))
                (btset/slice (.-avet db) (resolve-datom db nil attr start nil)
                             (resolve-datom db nil attr end nil))))

(defn db? [x] (and (instance? ISearch x) (instance? IIndexAccess x) (instance? IDB x)))

;; ----------------------------------------------------------------------------
(defrecord-updatable FilteredDB [unfiltered-db pred #?(:clj __hash)]
  #?@(:cljs
      [IHash                (-hash  [db]        (hash-fdb db))
       IEquiv               (-equiv [db other]  (equiv-db db other))
       ISeqable             (-seq   [db]        (-datoms db :eavt []))
       ICounted             (-count [db]        (count (-datoms db :eavt [])))
       IPrintWithWriter     (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_]         (throw (js/Error. "-empty is not supported on FilteredDB")))

       ILookup              (-lookup ([_ _]     (throw (js/Error. "-lookup is not supported on FilteredDB")))
                                     ([_ _ _]   (throw (js/Error. "-lookup is not supported on FilteredDB"))))


       IAssociative         (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on FilteredDB")))
                            (-assoc [_ _ _]       (throw (js/Error. "-assoc is not supported on FilteredDB")))]

      :clj
      [Object               (hashCode [db]      (hash-fdb db))

       clojure.lang.IHashEq (hasheq [db]        (hash-fdb db))

       clojure.lang.IPersistentCollection
                            (count [db]         (count (-datoms db :eavt [])))
                            (equiv [db o]       (equiv-db db o))
                            (cons [db [k v]]    (throw (UnsupportedOperationException. "cons is not supported on FilteredDB")))
                            (empty [db]         (throw (UnsupportedOperationException. "empty is not supported on FilteredDB")))

       clojure.lang.Seqable (seq [db]           (-datoms db :eavt []))

       clojure.lang.ILookup (valAt [db k]       (throw (UnsupportedOperationException. "valAt/2 is not supported on FilteredDB")))
                            (valAt [db k nf]    (throw (UnsupportedOperationException. "valAt/3 is not supported on FilteredDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                (throw (UnsupportedOperationException. "getLookupThunk is not supported on FilteredDB")))

       clojure.lang.Associative
                            (containsKey [e k]  (throw (UnsupportedOperationException. "containsKey is not supported on FilteredDB")))
                            (entryAt [db k]     (throw (UnsupportedOperationException. "entryAt is not supported on FilteredDB")))
                            (assoc [db k v]     (throw (UnsupportedOperationException. "assoc is not supported on FilteredDB")))])

  IDB
  (-schema [db] (-schema (.-unfiltered-db db)))
  (-attrs-by [db property] (-attrs-by (.-unfiltered-db db) property))

  ISearch
  (-search [db pattern]
           (filter (.-pred db) (-search (.-unfiltered-db db) pattern)))

  IIndexAccess
  (-datoms [db index cs]
           (filter (.-pred db) (-datoms (.-unfiltered-db db) index cs)))

  (-seek-datoms [db index cs]
                (filter (.-pred db) (-seek-datoms (.-unfiltered-db db) index cs)))

  (-index-range [db attr start end]
                (filter (.-pred db) (-index-range (.-unfiltered-db db) attr start end))))

;; ----------------------------------------------------------------------------

(defn attr->properties [k v]
  (cond
    (= [k v] [:db/isComponent true]) [:db/isComponent]
    (= v :db.type/ref)               [:db.type/ref]
    (= v :db.cardinality/many)       [:db.cardinality/many]
    (= v :db.unique/identity)        [:db/unique :db.unique/identity]
    (= v :db.unique/value)           [:db/unique :db.unique/value]))

(defn- multimap [e m]
  (reduce
   (fn [acc [k v]]
     (update-in acc [k] (fnil conj e) v))
   {} m))

(defn- rschema [schema]
  (->>
   (for [[a kv] schema
         [k v]  kv
         prop   (attr->properties k v)]
     [prop a])
   (multimap #{})))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error :schema/validation
                     :attribute a
                     :key k
                     :value v}))))

(defn- validate-schema [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
        (throw (ex-info (str "Bad attribute specification for " a ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}")
                        {:error     :schema/validation
                         :attribute a
                         :key       :db/isComponent}))))
    (validate-schema-key a :db/unique (:db/unique kv) #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv) #{:db.type/ref})
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many}))
  schema)

(defn ^DB empty-db
  ([] (empty-db default-schema))
  ([schema]
    (map->DB {
      :schema  (validate-schema schema)
      :eavt    (btset/btset-by cmp-datoms-eavt)
      :aevt    (btset/btset-by cmp-datoms-aevt)
      :avet    (btset/btset-by cmp-datoms-avet)
      :max-eid 0
      :max-tx  tx0
      :rschema (rschema schema)
      #?@(:clj [:__hash (atom nil)])})))

(defn ^DB init-db
  ([datoms] (init-db datoms default-schema))
  ([datoms schema]
    (if (empty? datoms)
      (empty-db schema)
      (let [_ (validate-schema schema)
            #?@(:cljs
                [ds-arr  (into-array datoms)
                 eavt    (btset/-btset-from-sorted-arr (.sort ds-arr cmp-datoms-eavt-quick) cmp-datoms-eavt)
                 aevt    (btset/-btset-from-sorted-arr (.sort ds-arr cmp-datoms-aevt-quick) cmp-datoms-aevt)
                 avet    (btset/-btset-from-sorted-arr (.sort ds-arr cmp-datoms-avet-quick) cmp-datoms-avet)
                 max-eid (:e (first (-rseq eavt)))]
                :clj
                [eavt    (apply btset/btset-by cmp-datoms-eavt datoms)
                 aevt    (apply btset/btset-by cmp-datoms-aevt datoms)
                 avet    (apply btset/btset-by cmp-datoms-avet datoms)
                 max-eid (:e (first (rseq eavt)))])
            max-tx (transduce (map (fn [^Datom d] (.-tx d))) max tx0 eavt)]
        (map->DB {
          :schema  schema
          :eavt    eavt
          :aevt    aevt
          :avet    avet
          :max-eid max-eid
          :max-tx  max-tx
          :rschema (rschema schema)
          #?@(:clj [:__hash (atom nil)])})))))

(defn- equiv-db-index [x y]
  (and (= (count x) (count y))
    (loop [xs (seq x)
           ys (seq y)]
      (cond
        (nil? xs) true
        (= (first xs) (first ys)) (recur (next xs) (next ys))
        :else false))))

(defn- hash-db [^DB db]
  #?(:cljs
     (or (.-__hash db)
         (set!   (.-__hash db) (hash-ordered-coll (-datoms db :eavt []))))
     :clj
     (or @(.-__hash db)
         (reset! (.-__hash db) (hash-ordered-coll (-datoms db :eavt []))))))

(defn- hash-fdb [^FilteredDB db]
  #?(:cljs
     (or (.-__hash db)
         (set!   (.-__hash db) (hash-ordered-coll (-datoms db :eavt []))))
     :clj
     (or @(.-__hash db)
         (reset! (.-__hash db) (hash-ordered-coll (-datoms db :eavt []))))))

(defn- equiv-db [db other]
  (and (or (instance? DB other) (instance? FilteredDB other))
       (= (-schema db) (-schema other))
       (equiv-db-index (-datoms db :eavt []) (-datoms other :eavt []))))

#?(:cljs
   (defn pr-db [db w opts]
     (-write w "#datascript/DB {")
     (-write w ":schema ")
     (pr-writer (-schema db) w opts)
     (-write w ", :datoms ")
     (pr-sequential-writer w
                           (fn [d w opts]
                             (pr-sequential-writer w pr-writer "[" " " "]" opts [(.-e d) (.-a d) (.-v d) (.-tx d)]))
                           "[" " " "]" opts (-datoms db :eavt []))
     (-write w "}")))

#?(:clj
   (do
     (defn pr-db [db, ^java.io.Writer w]
       (.write w (str "#datascript/DB {"))
       (.write w ":schema ")
       (binding [*out* w]
         (pr (-schema db))
         (.write w ", :datoms [")
         (apply pr (map (fn [^Datom d] [(.-e d) (.-a d) (.-v d) (.-tx d)]) (-datoms db :eavt []))))
       (.write w "]}"))

     (defmethod print-method DB [db, ^java.io.Writer w]
       (pr-db db w))

     (defmethod print-method FilteredDB [db, ^java.io.Writer w]
       (pr-db db w))))

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (Datom. e a v tx true)) datoms) schema))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom [db e a v t]
  (when a (validate-attr a (list 'resolve-datom 'db e a v t)))
  (Datom.
    (entid-some db e)         ;; e
    a                               ;; a
    (if (and (some? v) (ref? db a)) ;; v
      (entid-strict db v)
      v)
    (entid-some db t)         ;; t
    nil))

(defn- components->pattern [db index [c0 c1 c2 c3]]
  (case index
    :eavt (resolve-datom db c0 c1 c2 c3)
    :aevt (resolve-datom db c1 c0 c2 c3)
    :avet (resolve-datom db c2 c0 c1 c3)))

;; ----------------------------------------------------------------------------

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(defn #?@(:clj  [^Boolean is-attr?]
          :cljs [^boolean is-attr?]) [db attr property]
  (contains? (-attrs-by db property) attr))

(defn #?@(:clj  [^Boolean multival?]
          :cljs [^boolean multival?]) [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn #?@(:clj  [^Boolean ref?]
          :cljs [^boolean ref?]) [db attr]
  (is-attr? db attr :db.type/ref))

(defn #?@(:clj  [^Boolean component?]
          :cljs [^boolean component?]) [db attr]
  (is-attr? db attr :db/isComponent))

(defn entid [db eid]
  (cond
    (number? eid) eid
    (sequential? eid)
      (cond
        (not= (count eid) 2)
          (raise "Lookup ref should contain 2 elements: " eid
                 {:error :lookup-ref/syntax, :entity-id eid})
        (not (is-attr? db (first eid) :db.unique/identity))
          (raise "Lookup ref attribute should be marked as :db.unique/identity: " eid
                 {:error :lookup-ref/unique
                  :entity-id eid})
        :else
          (:e (first (-datoms db :avet eid))))
   :else
     (raise "Expected number or lookup ref for entity id, got " eid
             {:error :entity-id/syntax
              :entity-id eid})))

(defn entid-strict [db eid]
  (or (entid db eid)
      (raise "Nothing found for entity id " eid
             {:error :entity-id/missing
              :entity-id eid})))

(defn entid-some [db eid]
  (when eid
    (entid-strict db eid)))

;;;;;;;;;; Transacting

(defn validate-datom [db ^Datom datom]
  (when (and (.-added datom)
             (is-attr? db (.-a datom) :db/unique))
    (when-let [found (not-empty (-datoms db :avet [(.-a datom) (.-v datom)]))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error :transact/unique
              :attribute (.-a datom)
              :datom datom}))))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at]
  (when-not (or (keyword? attr) (string? attr))
    (raise "Bad entity attribute " attr " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

(defn- next-eid [db]
  (inc (:max-eid db)))

(defn- advance-max-eid [db eid]
  (cond-> db
    (and (> eid (:max-eid db))
         (< eid tx0)) ;; do not trigger advance if transaction id was referenced
      (assoc :max-eid eid)))

(defn- allocate-eid
  ([report eid]
    (update-in report [:db-after] advance-max-eid eid))
  ([report e eid]
    (-> report
      (assoc-in [:tempids e] eid)
      (update-in [:db-after] advance-max-eid eid))))

(defn- #?@(:clj  [^Boolean tx-id?]
           :cljs [^boolean tx-id?]) [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx"))) ;; for datascript.js interop

;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)

(defn- with-datom [db ^Datom datom]
  (validate-datom db datom)
  (if (.-added datom)
    (-> db
      (update-in [:eavt] btset/btset-conj datom cmp-datoms-eavt-quick)
      (update-in [:aevt] btset/btset-conj datom cmp-datoms-aevt-quick)
      (update-in [:avet] btset/btset-conj datom cmp-datoms-avet-quick)
      (advance-max-eid (.-e datom)))
    (let [removing (first (-search db [(.-e datom) (.-a datom) (.-v datom)]))]
      (-> db
        (update-in [:eavt] btset/btset-disj removing cmp-datoms-eavt-quick)
        (update-in [:aevt] btset/btset-disj removing cmp-datoms-aevt-quick)
        (update-in [:avet] btset/btset-disj removing cmp-datoms-avet-quick)))))

(defn- transact-report [report datom]
  (-> report
      (update-in [:db-after] with-datom datom)
      (update-in [:tx-data] conj datom)))

(defn #?@(:clj  [^Boolean reverse-ref?]
          :cljs [^boolean reverse-ref?]) [attr]
  (cond
    (keyword? attr)
    (= \_ (nth (name attr) 0))
    
    (string? attr)
    (boolean (re-matches #"(?:([^/]+)/)?_([^/]+)" attr))
   
    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn reverse-ref [attr]
  (cond
    (keyword? attr)
    (if (reverse-ref? attr)
      (keyword (namespace attr) (subs (name attr) 1))
      (keyword (namespace attr) (str "_" (name attr))))

   (string? attr)
   (let [[_ ns name] (re-matches #"(?:([^/]+)/)?([^/]+)" attr)]
     (if (= \_ (nth name 0))
       (if ns (str ns "/" (subs name 1)) (subs name 1))
       (if ns (str ns "/_" name) (str "_" name))))
   
   :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn- resolve-upsert [db entity]
  (if-let [idents (not-empty (-attrs-by db :db.unique/identity))]
    (reduce-kv
      (fn [ent a v]
        (if-let [^Datom datom (first (-datoms db :avet [a v]))]
          (let [old-eid (:db/id ent)
                new-eid (.-e datom)]
            (cond
              (nil? old-eid)
                (-> ent (dissoc a) (assoc :db/id new-eid)) ;; replace upsert attr with :db/id
              (= old-eid new-eid)
                (dissoc ent a) ;; upsert attr already in db
              :else              
                (raise "Cannot resolve upsert for " entity ": " {:db/id old-eid a v} " conflicts with existing " datom
                       {:error     :transact/upsert
                        :attribute a
                        :entity    entity
                        :datom     datom })))
          ent))
      entity
      (select-keys entity idents))
    entity))

;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a vs]
  (cond
    ;; not a multival context
    (not (or (reverse-ref? a)
             (multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or (array? vs)
             (and (coll? vs) (not (map? vs)))))
    [vs]
    
    ;; probably lookup ref
    (and (= (count vs) 2)
         (is-attr? db (first vs) :db.unique/identity))
    [vs]
    
    :else vs))


(defn- explode [db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] entity
          :when  (not= a :db/id)
          :let   [_          (validate-attr a {:db/id eid, a vs})
                  reverse?   (reverse-ref? a)
                  straight-a (if reverse? (reverse-ref a) a)
                  _          (when (and reverse? (not (ref? db straight-a)))
                               (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                      {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}}))]
          v      (maybe-wrap-multival db a vs)]
      (if (and (ref? db straight-a) (map? v)) ;; another entity specified as nested map
        (assoc v (reverse-ref a) eid)
        (if reverse?
          [:db/add v   straight-a eid]
          [:db/add eid straight-a v])))))

(defn- transact-add [report [_ e a v :as ent]]
  (validate-attr a ent)
  (validate-val  v ent)
  (let [tx    (current-tx report)
        db    (:db-after report)
        e     (entid-strict db e)
        v     (if (ref? db a) (entid-strict db v) v)
        datom (Datom. e a v tx true)]
    (if (multival? db a)
      (if (empty? (-search db [e a v]))
        (transact-report report datom)
        report)
      (if-let [^Datom old-datom (first (-search db [e a]))]
        (if (= (.-v old-datom) v)
          report
          (-> report
            (transact-report (Datom. e a (.-v old-datom) tx false))
            (transact-report datom)))
        (transact-report report datom)))))

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (Datom. (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (comp
              (filter (fn [^Datom d] (component? db (.-a d))))
              (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(defn transact-tx-data [report es]
  (when-not (or (nil? es) (sequential? es))
    (raise "Bad transaction data " es ", expected sequential collection"
           {:error :transact/syntax, :tx-data es}))
  (let [[entity & entities] es
        db (:db-after report)]
    (cond
      (nil? entity)
        (-> report
            (assoc-in  [:tempids :db/current-tx] (current-tx report))
            (update-in [:db-after :max-tx] inc))

      (map? entity)
        (let [old-eid      (:db/id entity)
              known-eid    (->> 
                             (cond
                               (neg-number? old-eid) (get-in report [:tempids old-eid])
                               (tx-id? old-eid)      (current-tx report)
                               :else                 old-eid)
                             (entid-some db))
              upserted     (resolve-upsert db (assoc entity :db/id known-eid))
              new-eid      (or (:db/id upserted) (next-eid db))
              new-entity   (assoc upserted :db/id new-eid)
              new-report   (cond
                             (nil? old-eid) (allocate-eid report new-eid)
                             (neg-number? old-eid) (allocate-eid report old-eid new-eid)
                             :else report)]
          (recur new-report (concat (explode db new-entity) entities)))

      (sequential? entity)
        (let [[op e a v] entity]
          (cond
            (= op :db.fn/call)
              (let [[_ f & args] entity]
                (recur report (concat (apply f db args) entities)))

            (= op :db.fn/cas)
              (let [[_ e a ov nv] entity
                    e (entid-strict db e)
                    _ (validate-attr a entity)
                    ov (if (ref? db a) (entid-strict db ov) ov)
                    nv (if (ref? db a) (entid-strict db nv) nv)
                    _ (validate-val ov entity)
                    _ (validate-val nv entity)
                    datoms (-search db [e a])]
                (if (multival? db a)
                  (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
                    (recur (transact-add report [:db/add e a nv]) entities)
                    (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                           {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                  (let [v (:v (first datoms))]
                    (if (= v ov)
                      (recur (transact-add report [:db/add e a nv]) entities)
                      (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                             {:error :transact/cas, :old (first datoms), :expected ov, :new nv })))))
           
            (tx-id? e)
              (recur report (concat [[op (current-tx report) a v]] entities))
           
            (and (ref? db a) (tx-id? v))
              (recur report (concat [[op e a (current-tx report)]] entities))

            (neg-number? e)
              (if-let [eid (get-in report [:tempids e])]
                (recur report (concat [[op eid a v]] entities))
                (recur (allocate-eid report e (next-eid db)) es))

            (and (ref? db a) (neg-number? v))
              (if-let [vid (get-in report [:tempids v])]
                (recur report (concat [[op e a vid]] entities))
                (recur (allocate-eid report v (next-eid db)) es))

            (= op :db/add)
              (recur (transact-add report entity) entities)

            (= op :db/retract)
              (let [e (entid-strict db e)
                    v (if (ref? db a) (entid-strict db v) v)]
                (validate-attr a entity)
                (validate-val v entity)
                (if-let [old-datom (first (-search db [e a v]))]
                  (recur (transact-retract-datom report old-datom) entities)
                  (recur report entities)))

            (= op :db.fn/retractAttribute)
              (let [e (entid-strict db e)]
                (validate-attr a entity)
                (let [datoms (-search db [e a])]
                  (recur (reduce transact-retract-datom report datoms)
                         (concat (retract-components db datoms) entities))))

            (= op :db.fn/retractEntity)
              (let [e (entid-strict db e)
                    e-datoms (-search db [e])
                    v-datoms (mapcat (fn [a] (-search db [nil a e])) (-attrs-by db :db.type/ref))]
                (recur (reduce transact-retract-datom report (concat e-datoms v-datoms))
                       (concat (retract-components db e-datoms) entities)))
           
           :else
             (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute or :db.fn/retractEntity"
                    {:error :transact/syntax, :operation op, :tx-data entity})))
     :else
       (raise "Bad entity type at " entity ", expected map or vector"
              {:error :transact/syntax, :tx-data entity})
     )))

