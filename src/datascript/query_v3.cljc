(ns datascript.query-v3
  (:require
    [clojure.set :as set]
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.lru :as lru]
    [datascript.shim :as shim]
    [datascript.parser :as dp #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple
                                                 Constant DefaultSrc Pattern RulesVar SrcVar Variable
                                                 Not Or And]])]
    [datascript.btset :as btset #?@(:cljs [:refer [Iter]])]
    [datascript.perf :as perf])
  #?(:clj
    (:import 
      [datascript.parser
        BindColl BindIgnore BindScalar BindTuple
        Constant DefaultSrc Pattern RulesVar SrcVar Variable
        Not Or And]
      [clojure.lang     IReduceInit Counted]
      [datascript.core  Datom]
      [datascript.btset Iter])))

(declare resolve-clauses collect)

(def ^:const lru-cache-size 100)

(defn mapa [f coll]
  (shim/into-array (map f coll)))

(defn arange [start end]
  (shim/into-array (range start end)))

(defn subarr [arr start end]
  (shim/acopy arr start end (shim/make-array (- end start)) 0))

(defn concatv [& xs]
  (into [] cat xs))

(defprotocol NativeColl
  (-native-coll [_]))

(defn native-coll [xs]
  (if (satisfies? NativeColl xs)
    (-native-coll xs)
    xs))

(defn fast-map []
  #?(:clj
     (let [m (java.util.HashMap.)]
       (reify
         NativeColl
         (-native-coll [_] m)
         
         clojure.lang.IEditableCollection
         (asTransient [this] this)
         
         clojure.lang.ITransientAssociative
         (assoc [this k v] (.put m k v) this)
         
         clojure.lang.ITransientCollection
         (persistent [this] this)
         
         clojure.lang.IPersistentCollection
         clojure.lang.Counted
         (count [_] (.size m))
         
         clojure.lang.ILookup
         (valAt [_ k] (.get m k))
         (valAt [_ k nf] (or (.get m k) nf))))
     :cljs {}))

(defn fast-arr []
  #?(:clj
     (let [l (java.util.ArrayList.)]
       (reify
         NativeColl
         (-native-coll [_] l)
         
         clojure.lang.IEditableCollection
         (asTransient [this] this)
         
         clojure.lang.ITransientCollection
         (conj [this v] (.add l v) this)
         (persistent [this] this)
         
         clojure.lang.IPersistentCollection
         clojure.lang.Counted
         (count [_] (.size l))
         
         clojure.lang.IReduceInit
         (reduce [_ f s]
           (loop [i   0
                  res s]
             (if (< i (.size l))
               (recur (inc i) (f res (.get l i)))
               res)))))
     :cljs
     (let [arr (shim/array)]
       (reify
         NativeColl
         (-native-coll [_] arr)
         
         IEditableCollection
         (-as-transient [this] this)
         
         ITransientCollection
         (-conj! [this v] (.push arr v) this)
         (-persistent! [this] this)
         
         ICounted
         (-count [_] (alength arr))
         
         IReduce
         (-reduce [_ f s]
           (loop [i   0
                  res s]
             (if (< i (alength arr))
               (recur (inc i) (f res (aget arr i)))
               res)))))))

(defn fast-set []
  #?(:clj
     (let [set (java.util.HashSet.)]
       (reify
         NativeColl
         (-native-coll [_] set)
         
         clojure.lang.IEditableCollection
         (asTransient [this] this)
         
         clojure.lang.ITransientCollection
         (conj [this v] (.add set v) this)
         (persistent [this] this)
         
         clojure.lang.IPersistentCollection
         clojure.lang.IPersistentSet
         (contains [_ k] (.contains set k))

         clojure.lang.Counted
         (count [_] (.size set))
         
         clojure.lang.IReduceInit
         (reduce [_ f s]
           (let [iter (.iterator set)]
             (loop [acc s]
               (if (.hasNext iter)
                 (recur (f acc (.next iter)))
                 acc))))))
     :cljs #{}))

;; (defrecord Context [rels consts sources rules default-source-symbol])

(def empty-context {:empty? true})

(defprotocol IRelation
  (-project    [_ syms])
  (-alter-coll [_ f])
  (-symbols    [_])
  (-arity      [_])
  (-fold       [_ f init])
  (-size       [_])
  (-getter     [_ symbol])
  (-indexes    [_ symbols])
  (-copy-tuple [_ tuple idxs target target-idxs]))

;; (defn- #?@(:clj  [^long hash-arr]
;;            :cljs [^number  hash-arr]) [arr]
;;   (let [count (int (shim/alength arr))]
;;     (loop [n         (int 0)
;;            hash-code (int 1)]
;;       (if (== n count)
;;         (mix-collection-hash hash-code n)
;;         (recur (inc n)
;;                #?(:clj  (unchecked-add-int
;;                           (unchecked-multiply-int 31 hash-code)
;;                           (hash (shim/aget arr n)))
;;                   :cljs (bit-or (+ (imul 31 hash-code)
;;                                    (hash (shim/aget arr n)))
;;                                 0)))))))

;; (declare equiv-tuple)

;; (deftype Tuple [arr hash]  
;;   #?@(
;;     :cljs [
;;       Object (equiv  [this other] (equiv-tuple this other))
;;       IHash  (-hash  [_]          hash)
;;       IEquiv (-equiv [this other] (equiv-tuple this other)) ]
;;     :clj [
;;       Object
;;       (hashCode [_]
;;         (java.util.Arrays/hashCode ^{:tag "[[Ljava.lang.Object;"} arr))
;;       (equals   [this other]
;;         (java.util.Arrays/equals ^{:tag "[[Ljava.lang.Object;"} arr
;;                                  ^{:tag "[[Ljava.lang.Object;"} (.-arr ^Tuple other)))
;;     ]))

;; (defn equiv-tuple [^Tuple t ^Tuple o]
;;   (boolean
;;     (and
;;       (== (shim/alength (.-arr t))
;;           (shim/alength (.-arr o)))
;;       (loop [i 0]
;;         (cond
;;           (== i (shim/alength (.-arr t))) true
;;           (not= (shim/aget (.-arr t) i) (shim/aget (.-arr o) i)) false
;;           :else (recur (inc i)))))))

;; (defn tuple [arr]
;;   (->Tuple arr (hash-arr arr)))

;;; ArrayRelation

(defn pr-rel [rel ^java.io.Writer w]
  (doto w
    (.write "#")
    (.write #?(:clj  (.getSimpleName ^Class (class rel))
               :cljs (str (type rel))))
    (.write "{:symbols ")
    (.write (pr-str (-symbols rel)))
    (.write ", :coll " )
    (.write (pr-str (persistent! (-fold rel #(conj! %1 (seq %2)) (transient [])))))
    (.write "}")))

(deftype ArrayRelation [offset-map coll]
  IRelation
  (-project [_ syms]
    (ArrayRelation. (select-keys offset-map syms) coll))
  (-alter-coll [_ f]
    (ArrayRelation. offset-map (f coll)))
  (-symbols [_] (keys offset-map))
  (-arity   [_] (count offset-map))
  (-fold [_ f init] (reduce f init coll))
  (-size [_] (count coll))
  (-getter [_ symbol]
    (let [idx (offset-map symbol)]
      (fn [tuple]
        (shim/aget tuple idx))))
  (-indexes [_ syms]
    (mapa offset-map syms))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (dotimes [i (shim/alength idxs)]
      (shim/aset target (shim/aget target-idxs i) (shim/aget tuple (shim/aget idxs i))))))

#?(:clj
   (defmethod print-method ArrayRelation [rel w]
     (pr-rel rel w)))

(defn array-rel [symbols coll]
  (->ArrayRelation (zipmap symbols (range)) coll))


;;; CollRelation

(deftype CollRelation [offset-map coll]
  IRelation
  (-project [_ syms]
    (CollRelation. (select-keys offset-map syms) coll))
  (-alter-coll [_ f]
    (CollRelation. offset-map (f coll)))
  (-symbols [_]        (keys offset-map))
  (-arity   [_]        (count offset-map))
  (-fold    [_ f init] (reduce f init coll))
  (-size    [_]        (if (instance? Iter coll)
                         (count coll);; (btset/est-count coll)
                         (count coll)))
  (-getter  [_ symbol]
    (let [idx (offset-map symbol)]
      (fn [tuple]
        (nth tuple idx))))
  (-indexes [_ syms]
    (mapa offset-map syms))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (dotimes [i (shim/alength idxs)]
      (shim/aset target (shim/aget target-idxs i) (nth tuple (shim/aget idxs i))))))

(defn coll-rel [symbols coll]
  (let [offset-map (reduce-kv
                     (fn [acc e i]
                       (cond
                         (instance? BindScalar e)
                           (assoc acc (get-in e [:variable :symbol]) i)
                         (instance? Variable e)
                           (assoc acc (:symbol e) i)
                         :else acc))
                     {}
                     (zipmap symbols (range)))]
    (->CollRelation offset-map coll)))

#?(:clj
   (defmethod print-method CollRelation [rel w]
     (pr-rel rel w)))


;;; ProdRelation

;; (deftype ProdRelation [rel1 rel2]  
;;   IRelation
;;   (-symbols [_] (concatv (-symbols rel1) (-symbols rel2)))
;;   (-arity   [_] (+ (-arity rel1) (-arity rel2)))
;;   (-fold [_ f init]
;;     (-fold rel1
;;            (fn [acc t1]
;;              (-fold rel2
;;                     (fn [acc t2]
;;                       (f acc (shim/array t1 t2)))
;;                     acc))
;;             init))

;;   (-size [_]
;;     (* (-size rel1) (-size rel2)))
;;   (-getter [_ symbol]
;;     (if (some #{symbol} (-symbols rel1))
;;       (let [getter (-getter rel1 symbol)]
;;         (fn [tuple]
;;           (getter (shim/aget tuple 0))))
;;       (let [getter (-getter rel2 symbol)]
;;         (fn [tuple]
;;           (getter (shim/aget tuple 1))))))
;;   (-indexes [_ syms]
;;     (let [[syms1 syms2] (split-with (set (-symbols rel1)) syms)]
;;       [(-indexes rel1 syms1)
;;        (count syms1)
;;        (-indexes rel2 syms2)
;;        (count syms2)]))
;;   (-copy-tuple [_ tuple idxs target target-idxs]
;;     (let [[idxs1 arity1 idxs2 arity2] idxs
;;           target-idxs1 (subarr target-idxs 0 arity1)
;;           target-idxs2 (subarr target-idxs arity1 (shim/alength target-idxs))]
;;       (-copy-tuple rel1 (shim/aget tuple 0) idxs1 target target-idxs1)
;;       (-copy-tuple rel2 (shim/aget tuple 1) idxs2 target target-idxs2))))

;; (def prod-rel ->ProdRelation)


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


(defn product-all [rels]
  (reduce product rels)) ;; TODO check for empty rels


;; union


(defn union [rel1 rel2]
  (throw (ex-info "Not implemented" [rel1 rel2])))


;; hash-join


(defn- key-fn [rel syms]
  (let [arity (count syms)]
    (if (== arity 1)
      (-getter rel (first syms))
      (let [idxs        (-indexes rel syms)
            target-idxs (arange 0 arity)]
        (fn [t]
          (let [arr (shim/make-array arity)]
            (-copy-tuple rel t idxs arr target-idxs)
            (vec arr)))))))


(defn hash-map-rel [rel syms]
  (let [key-fn (key-fn rel syms)]
    (->>
      (-fold rel
        (fn [hash t]
          (let [key (key-fn t)
                old (get hash key)]
            (if (nil? old)
              (assoc! hash key (conj! (fast-arr) t))
              (do (conj! old t) hash))))
        (transient (fast-map)))
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

;;; Bindings

(defn- bindable-to-seq? [x]
  (or (shim/seqable? x) (shim/array? x)))

(defn- plain-tuple-binding? [binding]
  (and (instance? BindTuple binding)
       (every? #(or (instance? BindScalar %)
                    (instance? BindIgnore %))
               (:bindings binding))))

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
          (coll-rel (dp/collect-vars-distinct binding) [])
       
        ;; [?x ...] type of binding
        (instance? BindScalar inner-binding)
          (coll-rel [inner-binding] coll)
       
        ;; [[?x ?y] ...] type of binding
        (plain-tuple-binding? inner-binding)
          (coll-rel (:bindings inner-binding) coll)

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
        (coll-rel (:bindings binding) [coll])
      ;; fallback to generic case
      :else
        (product-all
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

(defn get-source [context source]
  (let [symbol (cond
                 (instance? SrcVar source)     (:symbol source)
                 (instance? DefaultSrc source) (:default-source-symbol context)
                 :else (dc/raise "Source expected, got " source))]
    (or (get (:sources context) symbol)
        (dc/raise "Source " symbol " is not defined"
               {:error :query/where, :symbol symbol}))))
    

;; Patterns

(defn resolve-pattern-db [db clause]
  ;; TODO optimize with bound attrs min/max values here
  (let [pattern        (:pattern clause)
        search-pattern (mapv #(when (instance? Constant %) (:value %)) pattern)
        datoms         (dc/-search db search-pattern)]
    (coll-rel (:pattern clause) datoms)))


(defn- matches-pattern? [idxs tuple] ;; TODO handle repeated vars
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


(defn resolve-pattern-coll [coll clause]
  (when-not (bindable-to-seq? coll)
    (dc/raise "Cannot match by pattern " (dp/source clause) " because source is not a collection: " coll
       {:error :query/where, :value coll, :binding (dp/source clause)}))
  (let [pattern (:pattern clause)
        idxs    (->> (map #(when (instance? Constant %1) [%2 (:value %1)]) pattern (range))
                     (remove nil?)
                     (into {}))
        data    (filter #(matches-pattern? idxs %) coll)]
    (coll-rel pattern data)))


(defn clause-syms [clause]
  (into #{} (map :symbol) (dp/collect #(instance? Variable %) clause #{})))


(defn substitute-constants [clause context]
  (let [syms   (clause-syms clause)
        consts (:consts context)]
    (if (some #(contains? consts %) syms)
      (perf/measure
        (dp/postwalk
          clause
          (fn [form]
            (if (instance? Variable form)
              (let [sym (:symbol form)]
                (if-let [subs (get (:consts context) (:symbol form))]
                  (Constant. subs)
                  form))
              form)))
        "substitute-constants" (->> syms
                                    (filter #(contains? consts %))
                                    (map #(vector % (consts %)))
                                    (into {})))
      clause)))


(defn related-rels [context syms]
  (let [syms (set syms)]
    (->> (:rels context)
         (filter #(some syms (-symbols %))))))


(defn extract-rel [context syms]
  (let [syms      (set syms)
        rels      (:rels context)
        related?  #(some syms (-symbols %))
        related   (filter related? rels)]
    (if (empty? related)
      [nil context]
      (let [unrelated (remove related? rels)]
        [(product-all related) (assoc context :rels unrelated)]))))


(defn join-unrelated [context rel]
  (case (long (-size rel))
    0 empty-context
    1 (do
        (perf/debug "Promoting to :consts" (rel->consts rel))
        (update context :consts merge (rel->consts rel)))
    (update context :rels conj rel)))


(defn hash-join-rel [context rel]
  (if (== 0 (-size rel))
    empty-context
    (let [syms                   (set (-symbols rel))
          [related-rel context*] (extract-rel context syms)]
      (if (nil? related-rel)
        (join-unrelated context rel)
        (perf/measure
          (let [join-syms   (set/intersection syms (set (-symbols related-rel)))
                _           (perf/debug (-symbols rel)
                                        "to" (-symbols related-rel)
                                        "over" join-syms)
                hash        (perf/measure (hash-map-rel related-rel join-syms)
                                          "hash calculated with" (count %) "keys")
                 ;; TODO choose between hash-join and lookup-join
                rel*        (perf/measure (hash-join related-rel hash join-syms rel)
                                          "hash-join" (-size %) "tuples")]
            (join-unrelated context* rel*))
          "hash-join-rel")))))


(defn resolve-pattern [context clause]
  (perf/measure
    (let [clause* (substitute-constants clause context)
          rel     (let [source (get-source context (:source clause))]
                    (if (satisfies? dc/ISearch source)
                      (resolve-pattern-db   source clause*)
                      (resolve-pattern-coll source clause*)))]
      (hash-join-rel context rel))
    "resolve-pattern" (dp/source clause)))


(defn project-rel [rel syms]
  (let [rel-syms (set (-symbols rel))
        syms     (set syms)]
    (cond
      (set/subset? rel-syms syms) rel
      (empty? (set/intersection rel-syms syms)) nil
      :else (-project rel syms))))


(defn project-context [context syms]
  (assoc context
    :consts (select-keys (:consts context) syms)
    :rels   (into []
                  (comp (map #(project-rel % syms))
                        (remove nil?))
                  (:rels context))))


(defn collect-opt
  "Collects values if only one symbol, vecs if many (compatible with key-fn)"
  [context syms]
  (if (== 1 (count syms))
    (let [sym (first syms)]
      (if-let [val (get (:consts context) sym)]
        (into (fast-set) [val])
        (let [rel    (first (related-rels context [sym]))
              getter (-getter rel sym)]
          (->> (fast-set)
               (transient)
               (-fold rel
                      (fn [set tuple]
                        (conj! set (getter tuple))))
               (persistent!)))))
    (collect context syms)))


(defn subtract-from-rel [rel syms exclude-key-set]
  (perf/measure
    (let [key-fn1 (key-fn rel syms)
          pred    (fn [t1] (contains? exclude-key-set (key-fn1 t1)))]
      (-alter-coll rel #(into (fast-arr) (remove pred) %)))
    "From"        (-symbols rel) (str "(" (-size rel) " tuples)")
    "subtracting" (count exclude-key-set) "tuples by" syms))


(defn subtract-contexts [context1 context2 syms]
  (if (:empty? context2) ;; empty context2 means there’s nothing to subtract
    context1
    ;; ignoring constants inherited from context1
    (let [syms* (set/difference syms (keys (:consts context1)))]
      (if (empty? syms*) ;; join happened by constants only
        empty-context    ;; context2 is not-empty, meaning it satisfied constans,
                         ;; meaning we proved constants in context1 do not match
        (let [[rel1 context1*] (extract-rel context1 syms*)
              set2             (collect-opt context2 syms*)
              rel1*            (subtract-from-rel rel1 syms* set2)]
          (join-unrelated context1* rel1*))))))


(defn upd-default-source [context clause]
  (let [source (:source clause)]
    (if (instance? SrcVar source)
      (assoc context :default-source-symbol (:symbol source))
      context)))


(defn check-bound [context syms form]
  (let [context-syms (-> #{}
                         (into (keys (:consts context)))
                         (into (mapcat -symbols) (:rels context)))]
    (when-not (set/subset? syms context-syms)
      (let [missing (set/difference syms context-syms)]
        (throw (ex-info (str "Insufficient bindings: " missing " not bound in " form)
                        {:error :query/where
                         :form  form
                         :vars  missing}))))))
                           

(defn resolve-not [context clause]
  (perf/measure
    (let [{:keys [source vars clauses]} clause
          syms      (into #{} (map :symbol) vars)
          _         (check-bound context syms (dp/source clause))
          context*  (-> context
                      (project-context syms) ;; sub-context will only see symbols Not is joined by
                      (upd-default-source clause)
                      (resolve-clauses clauses))]
      (subtract-contexts context context* syms))
    "resolve-not" (dp/source clause)))


(defprotocol IClause
  (-resolve-clause [clause context]))


(extend-protocol IClause
  Pattern
  (-resolve-clause [clause context]
    (resolve-pattern context clause))
  Not
  (-resolve-clause [clause context]
    (resolve-not context clause)))


(defn println-context [context]
  (print "{:rels")
  (if (empty? (:rels context))
    (println "  []")
    (doseq [rel (:rels context)]
      (println " " rel)))
  (println "  :consts" (:consts context) "}"))
    


(defn resolve-clauses [context clauses]
  (reduce (fn [context clause]
            (let [context* (-resolve-clause clause context)]
              (if (:empty? context*)
                (reduced context*)
                (do
;;                   (println (dp/source clause) "=>")
;;                   (println-context context*)
                  context*))))
          context clauses))


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
    (if (:empty? context)
      (fast-set)
      (let [syms-indexed (vec (shim/zip syms (range)))
            specimen     (shim/make-array (count syms))
            _            (collect-consts syms-indexed specimen (:consts context))
            related-rels (related-rels context syms)
            xfs          (-> (map #(collect-rel-xf syms-indexed %) related-rels)
                             (concat [(map vec)]))]
        (into (fast-set) (apply comp xfs) [specimen])))
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
                     :sources {}
                     :rules   {}
                     :default-source-symbol '$ }
          context  (perf/measure (resolve-ins context (:in parsed-q) inputs)
                     "resolve-ins")
          context  (perf/measure (resolve-clauses context (:where parsed-q))
                     "resolve-clauses")
          syms     (concat (dp/find-vars (:find parsed-q))
                           (map :symbol (:with parsed-q)))]
      (native-coll (collect context syms)))
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
