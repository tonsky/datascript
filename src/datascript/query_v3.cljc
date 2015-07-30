(ns datascript.query-v3
  (:require
    [clojure.set :as set]
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.lru :as lru]
    [datascript.shim :as shim]
    [datascript.parser :as dp #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple
                                                 Constant DefaultSrc Pattern RulesVar SrcVar Variable]])]
    [datascript.btset :as btset #?@(:cljs [:refer [Iter]])]
    [datascript.perf :as perf])
  #?(:clj
    (:import 
      [datascript.parser
        BindColl BindIgnore BindScalar BindTuple
        Constant DefaultSrc Pattern RulesVar SrcVar Variable]
      [clojure.lang     IReduceInit Counted]
      [datascript.core  Datom]
      [datascript.btset Iter])))

(def ^:const lru-cache-size 100)

(defn mapa [f coll]
  (shim/into-array (map f coll)))

(defn arange [start end]
  (shim/into-array (range start end)))

(defn subarr [arr start end]
  (shim/acopy arr start end (shim/make-array (- end start)) 0))

(defn concatv [& xs]
  (into [] cat xs))

(defn fast-map []
  #?(:clj
     (let [m (java.util.HashMap.)]
       (reify
         clojure.lang.ITransientAssociative
         (assoc [this k v] (.put m k v) this)
         
         clojure.lang.ITransientCollection
         (persistent [_]
           (reify
             clojure.lang.IPersistentCollection
             clojure.lang.Counted
             (count [_] (.size m))
             clojure.lang.ILookup
             (valAt [_ k] (.get m k))))
         
         clojure.lang.ILookup
         (valAt [_ k] (.get m k))))
     :cljs
     (transient {})))

#_(defn fast-map []
  (transient {}))

#_(defn fast-arr []
  (let [l (volatile! (list))]
    (reify
      clojure.lang.ITransientCollection
      (conj [this v] (vswap! l conj v) this)
      (persistent [_] @l))))

#_(defn fast-arr []
  (transient []))

#_(defn fast-arr []
  (let [l (volatile! [])]
    (reify
      clojure.lang.ITransientCollection
      (conj [this v] (vswap! l conj v) this)
      (persistent [this] @l))))

(defn fast-arr []
  #?(:clj
     (let [l (java.util.ArrayList.)]
       (reify
         clojure.lang.ITransientCollection
         (conj [this v] (.add l v) this)
         (persistent [_]
           (reify
             clojure.lang.IPersistentCollection
             clojure.lang.Counted
             (count [_] (.size l))
             clojure.lang.IReduceInit
             (reduce [_ f s]
               (loop [i   0
                      res s]
                 (if (< i (.size l))
                   (recur (inc i) (f res (.get l i)))
                   res)))))))
     :cljs
     (let [arr (shim/array)]
       (reify
         ITransientCollection
         (-conj! [this v] (.push arr v) this)
         (-persistent! [_]
           (reify
             ICounted
             (-count [_] (alength arr))
             IReduce
             (-reduce [_ f s]
               (loop [i   0
                      res s]
                 (if (< i (alength arr))
                   (recur (inc i) (f res (aget arr i)))
                   res)))))))))

(defn into-set [xf el]
  #?(:clj
     (let [rf (xf #(doto ^java.util.HashSet %1 (.add %2)))]
       (rf (java.util.HashSet.) el))
     :cljs
     (into #{} xf [el])))


;; (defrecord Context [rels consts zeroes sources rules default-source-symbol])

(defprotocol IRelation
  (-symbols    [_])
  (-arity      [_])
  (-fold       [_ f init])
  (-size       [_])
  (-getter     [_ symbol])
  (-indexes    [_ symbols])
  (-copy-tuple [_ tuple idxs target target-idxs])
  (-union      [_ rel]))

(defn- #?@(:clj  [^long hash-arr]
           :cljs [^number  hash-arr]) [arr]
  (let [count (int (shim/alength arr))]
    (loop [n         (int 0)
           hash-code (int 1)]
      (if (== n count)
        (mix-collection-hash hash-code n)
        (recur (inc n)
               #?(:clj  (unchecked-add-int
                          (unchecked-multiply-int 31 hash-code)
                          (hash (shim/aget arr n)))
                  :cljs (bit-or (+ (imul 31 hash-code)
                                   (hash (shim/aget arr n)))
                                0)))))))

(declare equiv-tuple)

(deftype Tuple [arr hash]  
  #?@(
    :cljs [
      Object (equiv  [this other] (equiv-tuple this other))
      IHash  (-hash  [_]          hash)
      IEquiv (-equiv [this other] (equiv-tuple this other)) ]
    :clj [
      Object
      (hashCode [_] hash)
      (equals   [this other] (equiv-tuple this other)) ]))

(defn equiv-tuple [^Tuple t ^Tuple o]
  (boolean
    (and
      (== (shim/alength (.-arr t))
          (shim/alength (.-arr o)))
      (loop [i 0]
        (cond
          (== i (shim/alength (.-arr t))) true
          (not= (shim/aget (.-arr t) i) (shim/aget (.-arr o) i)) false
          :else (recur (inc i)))))))

(defn tuple [arr]
  (->Tuple arr (hash-arr arr)))

;;; ArrayRelation

(deftype ArrayRelation [symbols coll]
  #?@(:clj [
      Object
      (toString [_] (str "ArrayRelation#{:symbols " symbols ", :coll " coll "}")) ])
  IRelation
  (-symbols [_] symbols)
  (-arity   [_] (count symbols))
  (-fold [_ f init] (reduce f init coll))
  (-size [_] (count coll))
  (-getter [_ symbol]
    (let [idx (get (zipmap symbols (range)) symbol)]
      (fn [tuple]
        (shim/aget tuple idx))))
  (-indexes [_ syms]
    (mapa (zipmap symbols (range)) syms))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (dotimes [i (shim/alength idxs)]
      (shim/aset target (shim/aget target-idxs i) (shim/aget tuple (shim/aget idxs i)))))
  (-union [_ rel]
    (ArrayRelation. symbols (into coll (.-coll ^ArrayRelation rel)))))

#?(:clj
   (defmethod print-method ArrayRelation [^ArrayRelation rel, ^java.io.Writer w]
     (.write w (str "#datascript.query/ArrayRelation "))
     (binding [*out* w]
       (pr {:symbols (.-symbols rel)
            :coll    (mapv vec (.-coll rel))}))))


(def array-rel ->ArrayRelation)


;;; Coll1Relation

(deftype Coll1Relation [symbol coll]
  IRelation
  (-symbols [_]        [symbol])
  (-arity   [_]        1)
  (-fold    [_ f init] (reduce f init coll))
  (-size    [_]        (count coll))
  (-getter  [_ _]      (fn [el] el))
  (-indexes [_ _]      0)
  (-copy-tuple [_ el _ target target-idxs]
    (shim/aset target (shim/aget target-idxs 0) el))
  (-union [_ rel]
    (Coll1Relation. symbol (into coll (.-coll ^Coll1Relation rel)))))

(def coll1-rel ->Coll1Relation)


;;; CollRelation

(deftype CollRelation [symbols offset-map arity coll]
  IRelation
  (-symbols [_]        symbols)
  (-arity   [_]        arity)
  (-fold    [_ f init] (reduce f init coll))
  (-size    [_]        (count coll))
  (-getter  [_ symbol]
    (let [idx (offset-map symbol)]
      (fn [tuple]
        (nth tuple idx))))
  (-indexes [_ syms]
    (mapa offset-map syms))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (dotimes [i (shim/alength idxs)]
      (shim/aset target (shim/aget target-idxs i) (nth tuple (shim/aget idxs i)))))
  (-union [_ rel]
    (CollRelation. symbols offset-map arity (into coll (.-coll ^CollRelation rel)))))

(defn coll-rel [symbols coll]
  (let [offset-map (reduce-kv
                     (fn [acc s i]
                       (if (not= s '_)
                         (assoc acc s i)
                         acc))
                     {}
                     (zipmap symbols (range)))]
    (->CollRelation (keys offset-map) offset-map (count offset-map) coll)))


;;; ProdRelation

(deftype ProdRelation [rel1 rel2]  
  IRelation
  (-symbols [_] (concatv (-symbols rel1) (-symbols rel2)))
  (-arity   [_] (+ (-arity rel1) (-arity rel2)))
  (-fold [_ f init]
    (-fold rel1
           (fn [acc t1]
             (-fold rel2
                    (fn [acc t2]
                      (f acc (shim/array t1 t2)))
                    acc))
            init))

  (-size [_]
    (* (-size rel1) (-size rel2)))
  (-getter [_ symbol]
    (if (some #{symbol} (-symbols rel1))
      (let [getter (-getter rel1 symbol)]
        (fn [tuple]
          (getter (shim/aget tuple 0))))
      (let [getter (-getter rel2 symbol)]
        (fn [tuple]
          (getter (shim/aget tuple 1))))))
  (-indexes [_ syms]
    (let [[syms1 syms2] (split-with (set (-symbols rel1)) syms)]
      [(-indexes rel1 syms1)
       (count syms1)
       (-indexes rel2 syms2)
       (count syms2)]))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (let [[idxs1 arity1 idxs2 arity2] idxs
          target-idxs1 (subarr target-idxs 0 arity1)
          target-idxs2 (subarr target-idxs arity1 (shim/alength target-idxs))]
      (-copy-tuple rel1 (shim/aget tuple 0) idxs1 target target-idxs1)
      (-copy-tuple rel2 (shim/aget tuple 1) idxs2 target target-idxs2))))

(def prod-rel ->ProdRelation)


;;; EmptyRelation

(deftype EmptyRelation [symbols]
  IRelation
  (-symbols [_]        symbols)
  (-arity   [_]        (count symbols))
  (-fold    [_ f init] init)
  (-size    [_]        0)
  (-indexes [_ syms]
    (mapa (zipmap symbols (range)) syms)))

(def empty-rel ->EmptyRelation)


;;; SingletonRelation

(deftype SingletonRelation []
  IRelation
  (-symbols    [_] [])
  (-arity      [_] 0)
  (-fold       [_ f init] (f init (shim/into-array [])))
  (-size       [_] 1)
  (-indexes    [_ _] (shim/into-array []))
  (-copy-tuple [_ _ _ _ _]))

(def singleton-rel ->SingletonRelation)

;;; cartesian product

(defn- join-tuples [rel1 t1 idxs1
                    rel2 t2 idxs2
                    arity
                    target-idxs1 target-idxs2]
  (let [arr (shim/make-array arity)]
    (-copy-tuple rel1 t1 idxs1 arr target-idxs1)
    (-copy-tuple rel2 t2 idxs2 arr target-idxs2)
    arr))

(defn product [rel1 rel2]
  (perf/measure
    (let [idxs1  (-indexes rel1 (-symbols rel1))
          idxs2  (-indexes rel2 (-symbols rel2))
          arity1 (-arity rel1)
          arity2 (-arity rel2)
          arity  (+ arity1 arity2)
          target-idxs1 (arange 0 arity1)
          target-idxs2 (arange arity1 arity)
          coll   (-fold rel1
                        (fn [acc t1]
                          (-fold rel2
                                 (fn [acc t2]
                                   (conj! acc (join-tuples rel1 t1 idxs1
                                                           rel2 t2 idxs2
                                                           arity
                                                           target-idxs1
                                                           target-idxs2)))
                                 acc))
                        (fast-arr))]
      (array-rel
        (concatv (-symbols rel1) (-symbols rel2))
        (persistent! coll)))
    "product of" (-symbols rel1) (str "(" (-size rel1) " tuples)") "and" (-symbols rel2) (str "(" (-size rel2) " tuples)")))


;;; hash-join

(defn- key-fn [rel syms]
  (let [arity (count syms)]
    (if (== arity 1)
      (-getter rel (first syms))
      (let [idxs        (-indexes rel syms)
            target-idxs (arange 0 arity)]
        (fn [t]
          (let [arr (shim/make-array arity)]
            (-copy-tuple rel t idxs arr target-idxs)
            (tuple arr)))))))

(defn- common-symbols [rel1 rel2]
  (filter (set (-symbols rel2)) (-symbols rel1)))

(defn hash-rel [rel syms]
  (let [key-fn (key-fn rel syms)]
    (->>
      (-fold rel
        (fn [hash t]
          (let [key (key-fn t)
                old (get hash key)]
            (if (nil? old)
              (assoc! hash key (conj! (fast-arr) t))
              (do (conj! old t) hash))))
        (fast-map))
     (persistent!))))

(defn hash-join [rel1 hash1 join-syms rel2]
  (let [syms1       (-symbols rel1)
        syms2       (-symbols rel2)
        keep-syms2  (remove (set syms1) syms2)
        key-fn2     (key-fn rel2 join-syms)
        idxs1       (-indexes rel1 syms1)
        idxs2       (-indexes rel2 keep-syms2)
        full-syms   (concatv syms1 keep-syms2)
        arity1      (count syms1)
        arity2      (count keep-syms2)
        arity       (+ arity1 arity2)
        target-idxs1 (arange 0 arity1)
        target-idxs2 (arange arity1 arity)
        
        coll        (-fold rel2 ;; iterate over rel2
                      (fn [acc t2]
                        (let [tuples1 (get hash1 (key-fn2 t2))]
                          (if (nil? tuples1)
                            acc
                            (reduce (fn [acc t1]
                                      (conj! acc (join-tuples rel1 t1 idxs1
                                                              rel2 t2 idxs2
                                                              arity
                                                              target-idxs1
                                                              target-idxs2)))
                                    acc (persistent! tuples1)))))
                      (fast-arr))]
    (array-rel full-syms (persistent! coll))))


;;; Union

(defn union [rel1 rel2]
  (assert (= (type rel1) (type rel2)))
  (-union rel1 rel2))

;;; Bindings

(defn- bindable-to-seq? [x]
  (or (shim/seqable? x) (shim/array? x)))

(defn- binding-symbols [binding]
  (->> (dp/collect-vars-distinct binding)
       (map :symbol)))

(defn- plain-tuple-binding? [binding]
  (and (instance? BindTuple binding)
       (every? #(or (instance? BindScalar %)
                    (instance? BindIgnore %))
               (:bindings binding))))

(defn- bind-tuples [bindings coll]
  (coll-rel (mapv #(if (instance? BindScalar %)
                     (get-in % [:variable :symbol])
                     '_)
              bindings)
    coll))

(defprotocol IBinding
  (-in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (-in->rel [_ _]
    (singleton-rel))
  
  BindScalar
  (-in->rel [binding value]
    (array-rel [(get-in binding [:variable :symbol])] [(shim/array value)]))
  
  BindColl
  (-in->rel [binding coll]
    (let [inner-binding (:binding binding)]
      (cond
        (not (bindable-to-seq? coll))
          (dc/raise "Cannot bind value " coll " to collection " (dp/source binding)
                 {:error :query/binding, :value coll, :binding (dp/source binding)})
       
        (empty? coll)
          (empty-rel (binding-symbols binding))
       
        ;; [?x ...] type of binding
        (instance? BindScalar inner-binding)
          (coll1-rel (get-in inner-binding [:variable :symbol]) coll)
       
        ;; [[?x ?y] ...] type of binding
        (plain-tuple-binding? inner-binding)
          (bind-tuples (:bindings inner-binding) coll)

        ;; something more complex, fallback to generic case
        :else
          (reduce union
            (map #(-in->rel inner-binding %) coll)))))
  
  BindTuple
  (-in->rel [binding coll]
    (cond
      (not (bindable-to-seq? coll))
        (dc/raise "Cannot bind value " coll " to tuple " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      (< (count coll) (count (:bindings binding)))
        (dc/raise "Not enough elements in a collection " coll " to bind tuple " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      ;; [?x ?y] type of binding
      (plain-tuple-binding? binding)
        (bind-tuples (:bindings binding) [coll])
      ;; fallback to generic case
      :else
        (reduce product
          (map #(-in->rel %1 %2) (:bindings binding) coll)))))

(defn- rel->consts [rel]
  {:pre [(== (-size rel) 1)]}
  (let [tuple (-fold rel (fn [_ t] t) nil)]
    (into {}
      (map #(vector % ((-getter rel %) tuple)) (-symbols rel)))))

(defn- resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
      (update-in context [:sources] assoc (get-in binding [:variable :symbol]) value)
;;     (and (instance? BindScalar binding)
;;          (instance? RulesVar (:variable binding)))
;;       (assoc context :rules (parse-rules value))
    :else
      (let [rel (-in->rel binding value)]
        (if (== 1 (-size rel))
          (update-in context [:consts] merge (rel->consts rel))
          (update-in context [:rels] conj rel)))))

(defn resolve-ins [context bindings values]
  (when (not= (count bindings) (count values))
    (dc/raise "Wrong number of arguments for bindings " (mapv dp/source bindings)
           ", " (count bindings) " required, " (count values) " provided"
           {:error :query/binding, :binding (mapv dp/source bindings)}))
  (reduce resolve-in context (shim/zip bindings values)))

;;; Resolution

(defn substitute-constants [clause context]
  (perf/measure
    (dp/postwalk
      clause
      (fn [form]
        (if (instance? Variable form)
          (let [sym (:symbol form)]
            (if-let [subs (get (:consts context) (:symbol form))]
              (do
                (perf/debug "substituted" sym "with" subs)
                (Constant. subs))
              form))
          form)))
   "substitute-constants"))


(defn get-source [context source]
  (let [symbol (cond
                 (instance? SrcVar source)     (:symbol source)
                 (instance? DefaultSrc source) (:default-source-symbol context)
                 :else (dc/raise "Source expected, got " source))]
    (or (get (:sources context) symbol)
        (dc/raise "Source " symbol " is not defined"
               {:error :query/where, :symbol symbol}))))
    

;; Patterns

(deftype DatomsRelation [offset-map iter]
  IRelation
  (-symbols [_]        (keys offset-map))
  (-arity   [_]        (count offset-map))
  (-fold    [_ f init] (reduce f init iter))
  (-size    [_]        (if (instance? Iter iter)
                         (count iter);; (btset/est-count iter)
                         (count iter)))
  (-getter  [_ symbol]
    (let [idx (offset-map symbol)]
      (fn [datom]
        (nth datom idx))))
  (-indexes [_ syms]
    (mapa offset-map syms))
  (-copy-tuple [_ datom idxs target target-idxs]
    (dotimes [i (shim/alength idxs)]
      (shim/aset target
                 (shim/aget target-idxs i)
                 (nth datom (shim/aget idxs i)))))
                   
  ;;   (-union [_ rel]
  ;;     (CollRelation. symbols offsets arity (into coll (.-coll rel))))
  )

(defn- pattern->props-map [pattern]
  (reduce-kv
    (fn [acc var prop]
      (if (instance? Variable var)
        (assoc acc (:symbol var) prop)
        acc))
    {}
    (zipmap pattern (range 5))))

(defn resolve-pattern-db [db clause]
  ;; TODO optimize with bound attrs min/max values here
  (let [pattern        (:pattern clause)
        search-pattern (mapv #(when (instance? Constant %) (:value %)) pattern)
        datoms         (dc/-search db search-pattern)
        offset-map     (pattern->props-map (:pattern clause))]
    (->DatomsRelation offset-map datoms)))

(defn- matches-pattern? [idxs tuple]
;;   (when-not (bindable-to-seq? tuple)
;;     (dc/raise "Cannot match pattern " (dp/source clause) " because tuple is not a collection: " tuple
;;            {:error :query/where, :value tuple, :binding (dp/source clause)}))
;;   (when (< (count tuple) (count (:pattern clause)))
;;     (dc/raise "Not enough elements in a relation tuple " tuple " to match " (dp/source clause)
;;            {:error :query/where, :value tuple, :binding (dp/source clause)}))
  (reduce-kv
    (fn [_ i v]
      (if (not= (nth tuple i) v) ;; nth?
        (reduced false)
        true))
    true
    idxs))

(defn- bind-pattern [pattern coll]
  ;; TODO handle repeated vars
  (coll-rel (mapv #(if (instance? Variable %)
                     (:symbol %)
                     '_)
              pattern)
    coll))

(defn resolve-pattern-coll [coll clause]
  (when-not (bindable-to-seq? coll)
    (dc/raise "Cannot match by pattern " (dp/source clause) " because source is not a collection: " coll
       {:error :query/where, :value coll, :binding (dp/source clause)}))
  (let [pattern (:pattern clause)
        idxs    (->> (map #(when (instance? Constant %1) [%2 (:value %1)]) pattern (range))
                     (remove nil?)
                     (into {}))
        data    (filter #(matches-pattern? idxs %) coll)]
    (bind-pattern pattern data)))

(defn clause-syms [clause]
  (into #{} (map :symbol) (dp/collect #(instance? Variable %) clause #{})))

(defn split-rels [context syms]
  (let [related? #(some syms (-symbols %))]
    [(filter related? (:rels context))
     (remove related? (:rels context))]))

(defn join-empty [context syms]
  (let [[related unrelated] (split-rels context syms)
        related-syms        (mapcat -symbols related)]
    (perf/debug "Promoting to :empties" (set/union syms related-syms))
    (-> context
        (update :consts  #(apply dissoc % syms))
        (update :empties set/union syms related-syms)
        (assoc  :rels    unrelated))))

(defn join-constants [context rel]
  (perf/debug "Promoting to :consts" (rel->consts rel))
  (-> context
      (update :consts merge (rel->consts rel))))

(defn join-unrelated [context rel syms]
  (case (long (-size rel))
    0 (join-empty context syms)
    1 (join-constants context rel)
    (update context :rels conj rel)))
    
(defn hash-join-rel [context rel syms]
  (if (== 0 (-size rel))
    (join-empty context syms)
    (let [[related unrelated] (split-rels context syms)]
      (if (empty? related)
        (join-unrelated context rel syms)
        (perf/measure
         (let [join-syms   (into [] (comp (map -symbols) cat (filter syms)) related)
               _           (perf/debug "got" (count related) "related rels over" join-syms)
               _           (perf/when-debug
                            (doseq [rel related]
                              (perf/debug "  " (-symbols rel) "with" (-size rel) "tuples")))
               related-rel (reduce product related) ;; use prod-rel?
               hash        (perf/measure (hash-rel related-rel join-syms)
                                         "hash calculated with" (count %) "keys")
               rel*        (perf/measure (hash-join related-rel hash join-syms rel) ;; TODO choose between hash-join and lookup-join
                                         "hash-join to" (-symbols %) "with" (-size %) "tuples")
               context*    (assoc context :rels unrelated)
               syms*       (set/union syms (-symbols rel*))]
           (join-unrelated context* rel* syms*))
         "hash-join-rel")))))

(defn resolve-pattern [context clause]
  (perf/measure
    (let [syms    (clause-syms clause)
          consts  (:consts context)
          clause* (if (some #(contains? consts %) syms)
                    (substitute-constants clause context)
                    clause)
          rel     (if (some (:empties context) syms)
                    (empty-rel syms)
                    (let [source (get-source context (:source clause))]
                      (if (satisfies? dc/ISearch source)
                        (resolve-pattern-db   source clause*)
                        (resolve-pattern-coll source clause*))))]
      (hash-join-rel context rel syms))
    "resolve-pattern" (dp/source clause)))

(defprotocol IClause
  (-resolve-clause [_ context]))

(extend-protocol IClause
  Pattern
  (-resolve-clause [clause context]
    (resolve-pattern context clause)))

(defn resolve-clauses [context clauses]
  (reduce #(-resolve-clause %2 %1) context clauses))

(defn collect-consts [syms-indexed specimen consts]
  (doseq [[sym i] syms-indexed]
    (when (contains? consts sym)
      (let [val (get consts sym)]
        (shim/aset specimen i val)))))

(defn collect-rel-xf [syms-indexed rel]
  (let [sym+idx     (for [[sym i] syms-indexed
                            :when (shim/has? (-symbols rel) sym)]
                      [sym i])
        idxs        (-indexes rel (map first sym+idx))
        target-idxs (mapa second sym+idx)]
    (perf/debug "will collect" (-symbols rel) "with" (-size rel) "tuples")
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result specimen]
          (-fold rel
            (fn [acc tuple]
              (let [t (shim/aclone specimen)]
                (-copy-tuple rel tuple idxs t target-idxs)
                (rf acc t)))
            result))))))

(defn collect [context syms]
  (perf/measure
    (if (some (:empties context) syms)
      #{}
      (let [syms-indexed (vec (shim/zip syms (range)))
            specimen     (shim/make-array (count syms))
            _            (collect-consts syms-indexed specimen (:consts context))
            related-rels (->> (:rels context)
                              (filter #(some (set (-symbols %)) syms)))
            xfs          (-> (map #(collect-rel-xf syms-indexed %) related-rels)
                             (concat [(map vec)]))]
        (into-set (apply comp xfs) specimen)))
   "collect"))

;; Query

(def query-cache (volatile! (datascript.lru/lru lru-cache-size)))
(defn parse-query [q]
  (if-let [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))

(defn q [q & inputs]
  (perf/measure
    (let [parsed-q (perf/measure (parse-query q)
                            "parse-query")
          context  { :rels    []
                     :consts  {}
                     :empties #{}
                     :sources {}
                     :rules   {}
                     :default-source-symbol '$ }
          context  (perf/measure (resolve-ins context (:in parsed-q) inputs)
                     "resolve-ins")
          context  (perf/measure (resolve-clauses context (:where parsed-q))
                     "resolve-clauses")
          vars     (concat (dp/find-vars (:find parsed-q))
                           (map :symbol (:with parsed-q)))]
      (collect context vars))
    "Query" q))





;; (let [query   '[ :find  ?lid ?status ?starttime ?endtime (min ?paid) (distinct ?studentinfo) ?lgid
;;                  :in    $ ?tid ?week ?list
;;                  :where [?lid :lesson/teacherid ?tid]
;;                         [?lid :lesson/week ?week]
;;                         [?lid :lesson/lessongroupid ?lgid]
;;                         [?eid :enrollment/lessongroup_id ?lgid]
;;                         [?eid :enrollment/student_id ?sid]
;;                         [?iid :invoice/enrollment_id ?eid]
;;                         [?sid :student/firstname ?fname]
;;                         [?sid :student/lastname ?lname]
;;                         [?iid :invoice/paid ?paid]
;;                         [?lid :lesson/status ?status]
;;                         [?lid :lesson/starttime ?starttime]
;;                         [?lid :lesson/endtime ?endtime]
;;                         [(?list ?sid ?fname ?lname) ?studentinfo]]
;;       parsed (dp/parse-query query)]
;;   (perf/minibench "postwalk"
;;     (dp/postwalk parsed identity))
;;   (perf/minibench "parse-query"
;;     (dp/parse-query query))
;;   (perf/minibench "substitute-constants"
;;     (substitute-constants (first (:where parsed)) {:consts {'?tid 7}})))

;; (t/test-ns 'datascript.test.query-v3)

(defn random-man []
  {:name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
   :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
   :sex       (rand-nth [:male :female])
   :age       (rand-int 10)
   :salary    (rand-int 100000)})

(defn bench [name q & args]
  (println "\n---\n")
  (perf/minibench (str "OLD " name) (apply d/q q args))
  (perf/minibench (str "NEW " name) (apply datascript.query-v3/q q args))
  nil)

(comment
   (do
    #_(require '[datascript.query-v3 :as q] :reload)

    (def db (d/db-with (d/empty-db) (repeatedly 10000 random-man)))

    (bench "q2 const"
           '[:find  ?e
             :where [?e :name "Ivan"]
                    [?e :age 1]]
           db)

    #_(bench "q2 const in"
           '[:find ?e
             :in $ ?n
             :where [?e :name "Ivan"]
                    [?e :age ?n]]
           db 1)))
