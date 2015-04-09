(ns datascript.core
  (:require
    [#+cljs cljs.core #+clj clojure.core :as c]
    [datascript.btset :as btset]
    #+cljs [goog.array :as garray]
    #+clj [datascript.macros :refer [combine-cmp case-tree raise]])
  #+cljs
  (:require-macros
   [datascript.macros :refer [combine-cmp case-tree raise]]))

(def ^:const tx0 0x20000000)

(declare entid-strict entid-some ref? validate-attr -equiv-db -hash-db
         hash-datom equiv-datom seq-datom valAt-datom assoc-datom)

;;;
;;; Define Datom via deftype so that we can customize hash function to
;;; only use first three elements; original CLJS could do this since
;;; defrecord allows overrides in CLJS.
;;;
#+cljs
(deftype Datom [e a v tx added]
  Object
  (toString [d] (pr-str d))

  IHash
  (-hash [d] (or (.-__hash d)
                 (set! (.-__hash d) (hash-datom d))))
  IEquiv
  (-equiv [d o] (equiv-datom d o))

  ISeqable
  (-seq [d] (seq-datom d))

  ;; XXX fill in more of the CLJS api?
  )

#+clj
(deftype Datom [e a v tx added]
  Object
  (toString [d] (pr-str d))
  (hashCode [d] (hash-datom d))

  clojure.lang.Seqable
  (seq [d] (seq-datom d))

  clojure.lang.IPersistentCollection
  (entryAt [d k] (some->> (valAt-datom d k) (vector k)))
  (empty [d] false)
  (count [d] 5)
  (cons [d o] (throw (Exception. "cons'ing Datom."))) ; XXX

  clojure.lang.ILookup
  (valAt [d k] (valAt-datom d k nil))
  (valAt [d k not-found] (valAt-datom d k not-found))

  clojure.lang.Associative
  (equiv [d o] (equiv-datom d o))
  (containsKey [e k] (#{:e :a :v :tx :added} k))
  (assoc [d k v] (assoc-datom d k v))
  )

(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (hash-combine (hash (.-a d)))
      (hash-combine (hash (.-v d)))))

(defn- equiv-datom [^Datom d o]
  (and (= (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (= (.-v d) (.-v o))))

(defn- seq-datom [^Datom d]
 (list (.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)))

(defn- valAt-datom [^Datom d k & [not-found]]
  (case (name k) ; accept str, keyword and symbol
    "e"     (.-e d)
    "a"     (.-a d)
    "v"     (.-v d)
    "tx"    (.-tx d)
    "added" (.-added d)
    not-found))

(defn- assoc-datom [^Datom d k v]
  (throw (Exception. "assoc'ing Datom"))) ; XXX

(defmethod print-method Datom [d, ^java.io.Writer w]
  (.write w (str "#datascript.core/Datom ["))
  (binding [*out* w]
    (apply pr (map (partial get d) [:e :a :v :tx :added])))
  (.write w "]"))

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

(def neg-number? (every-pred number? neg?))

(defn- cmp [o1 o2]
  (if (and o1 o2)
    (compare o1 o2)
    0))

(defn- cmp-num [n1 n2]
  (if (and n1 n2)
    (- n1 n2)
    0))

#+cljs
(defn cmp-val [o1 o2]
  (if (and (some? o1) (some? o2))
    (let [t1 (type o1)
          t2 (type o2)]
      (if (identical? t1 t2)
        (compare o1 o2)
        (garray/defaultCompare t1 t2)))
    0))

#+clj
(defn cmp-val [o1 o2]
  (if (and (some? o1) (some? o2))
    (combine-cmp
     (compare (str (type o1)) (str (type o2)))
     (compare o1 o2))
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

;; see http://dev.clojure.org/jira/browse/CLJS-892
#+cljs
(defn- compare-keywords-quick [a b]
  (cond
    (identical? (.-fqn a) (.-fqn b)) 0
    (and (not (.-ns a)) (.-ns b)) -1
    (.-ns a) (if-not (.-ns b)
               1
               (let [nsc (garray/defaultCompare (.-ns a) (.-ns b))]
                 (if (zero? nsc)
                   (garray/defaultCompare (.-name a) (.-name b))
                   nsc)))
    :default (garray/defaultCompare (.-name a) (.-name b))))

#+cljs
(defn- cmp-attr-quick [a1 a2]
  ;; either both are keywords or both are strings
  (if (keyword? a1)
    (compare-keywords-quick a1 a2)
    (garray/defaultCompare a1 a2)))

#+cljs
(defn- cmp-val-quick [o1 o2]
  (let [t1 (type o1)
        t2 (type o2)]
    (if (identical? t1 t2)
      (compare o1 o2)
      (garray/defaultCompare t1 t2))))

#+cljs
(defn cmp-datoms-eavt-quick [d1 d2]
  (combine-cmp
    (- (.-e d1) (.-e d2))
    (cmp-attr-quick (.-a d1) (.-a d2))
    (cmp-val-quick  (.-v d1) (.-v d2))
    (- (.-tx d1) (.-tx d2))))

#+cljs
(defn cmp-datoms-aevt-quick [d1 d2]
  (combine-cmp
    (cmp-attr-quick (.-a d1) (.-a d2))
    (- (.-e d1) (.-e d2))
    (cmp-val-quick (.-v d1) (.-v d2))
    (- (.-tx d1) (.-tx d2))))

#+cljs
(defn cmp-datoms-avet-quick [d1 d2]
  (combine-cmp
    (cmp-attr-quick (.-a d1) (.-a d2))
    (cmp-val-quick  (.-v d1) (.-v d2))
    (- (.-e d1) (.-e d2))
    (- (.-tx d1) (.-tx d2))))

;; redefine cmps above as ptr to non-quick
#+clj
(do
  (def cmp-datoms-eavt-quick cmp-datoms-eavt)
  (def cmp-datoms-aevt-quick cmp-datoms-aevt)
  (def cmp-datoms-avet-quick cmp-datoms-avet))


(defn- resolve-datom [db e a v t]
  (when a (validate-attr a [e a v t]))
  (->Datom
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

;; ============================================================================

(declare db* equiv-db equiv-db-index hash-db valAt-db assoc-db)

#+clj
(deftype DB [schema eavt aevt avet max-eid max-tx rschema]
  Object
  (toString [d] (c/pr-str d))
  (hashCode [d] (hash-db d))

  clojure.lang.IPersistentCollection
  (empty [d] (empty? eavt))
  (count [d] (count eavt))
  (equiv [d o] (equiv-db d o))
  (cons [d [k v]] (assoc-db d k v))

  clojure.lang.Seqable
  (seq [d] (-datoms d :eavt []))

  clojure.lang.ILookup
  (valAt [d k] (valAt-db d k nil))
  (valAt [d k not-found] (valAt-db d k not-found))

  clojure.lang.Associative
  (containsKey [e k] (#{:e :a :v :tx :added} k))
  (entryAt [d k] (some->> (valAt-db d k) (vector k)))
  (assoc [d k v] (assoc-db d k v))

  ;; blank, filled in later
)

(defmethod print-method DB [db, ^java.io.Writer w]
  (.write w (str "#datascript.core/DB {:schema "))
  (binding [*out* w]
    (pr (-schema db))
    (.write w " :datoms ")
    (pr (-datoms db :eavt [])))
  (.write w "}"))

(defn pprint-db [db & [w]]
  (let [w (or w *out*)]
    (.write w "#datascript.core/DB {\n")
    (.write w "  :schema ")
    (print-method (-schema db) w)
    (.write w "\n  :datoms ")
    (doseq [datom (-datoms db :eavt [])]
      (.write w "\n    ")
      (print-method datom w))
    (.write w "\n  }\n")))

#+cljs
(defrecord DB [schema eavt aevt avet max-eid max-tx rschema]
  Object
  (toString [this]
    (#+cljs pr-str* #+clj c/pr-str this))

  clojure.lang.IHashEq
  (hasheq [this] (-hash-db this))

  clojure.lang.IPersistentCollection
  (equiv [this other] (-equiv-db this other)))
  
(extend-type DB
  IDB
  (-schema [this] (.-schema this))
  (-attrs-by [this property] ((.-rschema this) property))

  ISearch
  (-search [this [e a v tx]]
    (let [{:keys [eavt aevt avet]} this]
      (case-tree [e a (some? v) tx]
                 [(btset/slice eavt (->Datom e a v tx nil))                ;; e a v tx
                  (btset/slice eavt (->Datom e a v nil nil))               ;; e a v _
                  (->> (btset/slice eavt (->Datom e a nil nil nil))        ;; e a _ tx
                       (filter #(= tx (.-tx %))))
                  (btset/slice eavt (->Datom e a nil nil nil))             ;; e a _ _
                  (->> (btset/slice eavt (->Datom e nil nil nil nil))      ;; e _ v tx
                       (filter #(and (= v (.-v %)) (= tx (.-tx %)))))
                  (->> (btset/slice eavt (->Datom e nil nil nil nil))      ;; e _ v _
                       (filter #(= v (.-v %))))
                  (->> (btset/slice eavt (->Datom e nil nil nil nil))      ;; e _ _ tx
                       (filter #(= tx (.-tx %))))
                  (btset/slice eavt (->Datom e nil nil nil nil))           ;; e _ _ _
                  (->> (btset/slice avet (->Datom nil a v nil nil))        ;; _ a v tx
                       (filter #(= tx (.-tx %))))
                  (btset/slice avet (->Datom nil a v nil nil))             ;; _ a v _
                  (->> (btset/slice avet (->Datom nil a nil nil nil))      ;; _ a _ tx
                       (filter #(= tx (.-tx %))))
                  (btset/slice avet (->Datom nil a nil nil nil))           ;; _ a _ _
                  (filter #(and (= v (.-v %)) (= tx (.-tx %))) eavt)       ;; _ _ v tx
                  (filter #(= v (.-v %)) eavt)                             ;; _ _ v _
                  (filter #(= tx (.-tx %)) eavt)                           ;; _ _ _ tx
                  eavt])))                                                 ;; _ _ _ _

  IIndexAccess
  (-datoms [this index cs]
    (btset/slice (get this index) (components->pattern this index cs)))

  (-seek-datoms [this index cs]
    (btset/slice (get this index) (components->pattern this index cs) (->Datom nil nil nil nil nil)))

  (-index-range [this attr start end]
    (validate-attr attr nil)
    (btset/slice (:avet this)
                 (resolve-datom this nil attr start nil)
                 (resolve-datom this nil attr end nil))))

(defn- equiv-db-index [x y]
  (and (= (count x) (count y))
    (loop [xs (seq x)
           ys (seq y)]
      (cond
        (nil? xs) true
        (= (first xs) (first ys)) (recur (next xs) (next ys))
        :else false))))

(defn- hash-db [^DB db]
  #+cljs
  (or (.-__hash db)
      (set! (.-__hash db) (hash-coll (-datoms db :eavt []))))
  #+clj
  (hash-ordered-coll (-datoms db :eavt []))
  )

(defn- valAt-db [^DB db k & [not-found]]
  (case k
    :schema (.-schema db)
    :eavt (.-eavt db)
    :aevt (.-aevt db)
    :avet (.-avet db)
    :max-eid (.-max-eid db)
    :max-tx (.-max-tx db)
    :rschema (.-rschema db)
    not-found))

(defn- assoc-db [^DB db k v]
  (cond
    (= k :schema)
    (if (identical? (.-schema db) v)
      db
      (db* v (.-eavt db) (.-aevt db) (.-avet db) (.-max-eid db) (.-max-tx db) (.-rschema db)))

    (= k :eavt)
    (if (identical? (.-eavt db) v)
      db
      (db* (.-schema db) v (.-aevt db) (.-avet db) (.-max-eid db) (.-max-tx db) (.-rschema db)))

    (= k :aevt)
    (if (identical? (.-aevt db) v)
      db
      (db* (.-schema db) (.-eavt db) v (.-avet db) (.-max-eid db) (.-max-tx db) (.-rschema db)))

    (= k :avet)
    (if (identical? (.-avet db) v)
      db
      (db* (.-schema db) (.-eavt db) (.-aevt db) v (.-max-eid db) (.-max-tx db) (.-rschema db)))

    (= k :max-eid)
    (if (identical? (.-max-eid db) v)
      db
      (db* (.-schema db) (.-eavt db) (.-aevt db) (.-avet db) v (.-max-tx db) (.-rschema db)))

    (= k :max-tx)
    (if (identical? (.-max-tx db) v)
      db
      (db* (.-schema db) (.-eavt db) (.-aevt db) (.-avet db) (.-max-eid db) v (.-rschema db)))

    (= k :rschema)
    (if (identical? (.-rschema db) v)
      db
      (db* (.-schema db) (.-eavt db) (.-aevt db) (.-avet db) (.-max-eid db) (.-max-tx db) v))

    :else
    (throw (Exception. (str "Invalid key for #datascript.core.DB: " k)))
    ))

;; ----------------------------------------------------------------------------

(deftype FilteredDB [unfiltered-db pred]
  Object
  (toString [d] (c/pr-str d))
  (hashCode [d] (hash-db d))

  clojure.lang.IPersistentCollection
  (empty [d] (empty? (-datoms d :eavt [])))
  (count [d] (count (-datoms d :eavt [])))
  (equiv [d o] (equiv-db d o))
  (cons [d [k v]] (assoc-db d k v))

  clojure.lang.Seqable
  (seq [d] (-datoms d :eavt []))

  ;; XXX throw on usages below? (the idea appears to be that you use the API for these things)

  clojure.lang.ILookup
  ;;(valAt [d k] (valAt-db d k nil))
  ;;(valAt [d k not-found] (valAt-db d k not-found))

  clojure.lang.Associative
  ;;(containsKey [e k] (#{:e :a :v :tx :added} k))
  ;;(entryAt [d k] (some->> (valAt-db d k) (vector k)))
  ;;(assoc [d k v] (assoc-db d k v))

  )


#+cljs
(defrecord FilteredDB [unfiltered-db pred]
  Object
  (toString [this]
    (#+cljs pr-str* #+clj c/pr-str this))

  )
  

(extend-type FilteredDB
  IDB
  (-schema [fd] (-schema (.-unfiltered-db fd)))
  (-attrs-by [fd property] (-attrs-by (.-unfiltered-db fd) property))
  
  ISearch
  (-search [fd pattern]
    (filter (.-pred fd) (-search (.-unfiltered-db fd) pattern)))
  
  IIndexAccess
  (-datoms [fd index cs]
    (filter (.-pred fd) (-datoms (.-unfiltered-db fd) index cs)))

  (-seek-datoms [fd index cs]
    (filter (.-pred fd) (-seek-datoms (.-unfiltered-db fd) index cs)))

  (-index-range [fd attr start end]
    (filter (.-pred fd) (-index-range (.-unfiltered-db fd) attr start end))))

(defmethod print-method FilteredDB [f, ^java.io.Writer w]
  (.write w (str "#datascript.core/FilteredDB ["))
  (binding [*out* w]
    (pr (.-unfiltered-db f) (.-pred f)))
  (.write w "]"))

#+cljs
(extend-type DB
  IHash (-hash [this] (-hash-db this))
  IEquiv (-equiv [this other] (-equiv-db this other)))

#+cljs
(extend-type FilteredDB
  IHash (-hash [this] (-hash-db this))
  IEquiv (-equiv [this other] (-equiv-db this other)))


(defn attr->properties [k v]
  (cond
    (= [k v] [:db/isComponent true]) [:db/isComponent]
    (= v :db.type/ref)               [:db.type/ref]
    (= v :db.cardinality/many)       [:db.cardinality/many]
    (= v :db.unique/identity)        [:db/unique :db.unique/identity]
    (= v :db.unique/value)           [:db/unique :db.unique/value]))

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
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many})
  )
  schema)

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

(defn- db* [schema eavt aevt avet max-eid max-tx rschema]
  (DB. schema eavt aevt avet max-eid max-tx rschema))

(defn db [& [schema]]
  (db* (validate-schema schema)
       (btset/btset-by cmp-datoms-eavt)
       (btset/btset-by cmp-datoms-aevt)
       (btset/btset-by cmp-datoms-avet)
       0
       tx0
       (rschema schema)))

(def empty-db db)

(defn init-db [datoms & [schema]]
  (let [datoms  (into-array datoms)
        len     (alength datoms)
        eavt    (apply btset/btset-by cmp-datoms-eavt (seq datoms))
        aevt    (apply btset/btset-by cmp-datoms-aevt (seq datoms))
        avet    (apply btset/btset-by cmp-datoms-avet (seq datoms))
        max-eid (if (pos? len) (.-e (aget datoms (dec len))) 0)
        max-tx  (reduce max tx0 (map #(.-tx %) datoms))]
    (db*
     (validate-schema schema)
     eavt
     aevt
     avet
     max-eid
     max-tx
     (rschema schema))))

(defn- equiv-db [^DB this other]
  (and (or (instance? DB other) (instance? FilteredDB other))
       (= (-schema this) (-schema other))
       (equiv-db-index (-datoms this :eavt []) (-datoms other :eavt []))))

;; ============================================================================


(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(defn is-attr? [db attr property]
  (contains? (-attrs-by db property) attr))

(defn multival? [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn ref? [db attr]
  (is-attr? db attr :db.type/ref))

(defn component? [db attr]
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

(defn validate-datom [db datom]
  (when (and (:added datom)
             (is-attr? db (:a datom) :db/unique))
    (when-let [found (not-empty (-datoms db :avet [(:a datom) (:v datom)]))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error :transact/unique
              :attribute (:a datom)
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

(defn- tx-id? [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx"))) ;; for datascript.js interop

;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)

(defn- with-datom [db datom]
  (validate-datom db datom)
  (if (.-added datom)
    (-> db
      (update-in [:eavt] btset/btset-conj datom #+cljs cmp-datoms-eavt-quick)
      (update-in [:aevt] btset/btset-conj datom #+cljs cmp-datoms-aevt-quick)
      (update-in [:avet] btset/btset-conj datom #+cljs cmp-datoms-avet-quick)
      (advance-max-eid (.-e datom)))
    (let [removing (first (-search db [(.-e datom) (.-a datom) (.-v datom)]))]
      (-> db
          (update-in [:eavt] btset/btset-disj removing #+cljs cmp-datoms-eavt-quick)
          (update-in [:aevt] btset/btset-disj removing #+cljs cmp-datoms-aevt-quick)
          (update-in [:avet] btset/btset-disj removing #+cljs cmp-datoms-avet-quick)))))

(defn- transact-report [report datom]
  (-> report
      (update-in [:db-after] with-datom datom)
      (update-in [:tx-data] conj datom)))

(defn reverse-ref? [attr]
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
        (if-let [datom (first (-datoms db :avet [a v]))]
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

    ;; not a collection at all, so definetely a single value
    (not (or (#+cljs array? #+clj vector? vs)
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
        datom (->Datom e a v tx true)]
    (if (multival? db a)
      (if (empty? (-search db [e a v]))
        (transact-report report datom)
        report)
      (if-let [old-datom (first (-search db [e a]))]
        (if (= (.-v old-datom) v)
          report
          (-> report
            (transact-report (->Datom e a (.-v old-datom) tx false))
            (transact-report datom)))
        (transact-report report datom)))))

(defn- transact-retract-datom [report d]
  (let [tx (current-tx report)]
    (transact-report report (->Datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (->> datoms
                 ;; XXX this is already fixed in original tonsky code
                 (filter #(component? db (.-a %)))
                 (map #(vector :db.fn/retractEntity (.-v %))))))

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
              new-report   (if (neg-number? old-eid)
                             (allocate-eid report old-eid new-eid)
                             report)]
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
                  (if (some #(= (.-v %) ov) datoms)
                    (recur (transact-add report [:db/add e a nv]) entities)
                    (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                           {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                  (let [v (.-v (first datoms))] 
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

;; portable goo

#+clj
(defn array [len] (make-array Object len))
