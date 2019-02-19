(ns ^:no-doc datascript.query-v3
  (:require
    [clojure.set :as set]
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.query :as dq]
    [datascript.lru :as lru]
    [me.tonsky.persistent-sorted-set.arrays :as da]
    [datascript.parser :as dp #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple
                                                 Constant DefaultSrc Pattern RulesVar SrcVar Variable
                                                 Not Or And Predicate PlainSymbol]])])
  #?(:clj
    (:import 
      [datascript.parser
        BindColl BindIgnore BindScalar BindTuple
        Constant DefaultSrc Pattern RulesVar SrcVar Variable
        Not Or And Predicate PlainSymbol]
      [clojure.lang     IReduceInit Counted]
      [datascript.db  Datom])))

(declare resolve-clauses collect-rel-xf collect-to)

(def ^:const lru-cache-size 100)

(defn mapa [f coll]
  (to-array (map f coll)))

(defn arange [start end]
  (to-array (range start end)))

(defn subarr [arr start end]
  (da/acopy arr start end (da/make-array (- end start)) 0))

(defn concatv [& xs]
  (into [] cat xs))

(defn zip
  ([a b] (map vector a b))
  ([a b & rest] (apply map vector a b rest)))

(defn has? [coll el]
  (some #(= el %) coll))

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
     (let [arr (da/array)]
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
  (-copy-tuple [_ tuple idxs target target-idxs])
  (-union      [_ rel]))

;; (defn- #?@(:clj  [^long hash-arr]
;;            :cljs [^number  hash-arr]) [arr]
;;   (let [count (int (da/alength arr))]
;;     (loop [n         (int 0)
;;            hash-code (int 1)]
;;       (if (== n count)
;;         (mix-collection-hash hash-code n)
;;         (recur (inc n)
;;                #?(:clj  (unchecked-add-int
;;                           (unchecked-multiply-int 31 hash-code)
;;                           (hash (da/aget arr n)))
;;                   :cljs (bit-or (+ (imul 31 hash-code)
;;                                    (hash (da/aget arr n)))
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
;;       (== (da/alength (.-arr t))
;;           (da/alength (.-arr o)))
;;       (loop [i 0]
;;         (cond
;;           (== i (da/alength (.-arr t))) true
;;           (not= (da/aget (.-arr t) i) (da/aget (.-arr o) i)) false
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
        (da/aget tuple idx))))
  (-indexes [_ syms]
    (mapa offset-map syms))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (dotimes [i (da/alength idxs)]
      (da/aset target (da/aget target-idxs i) (da/aget tuple (da/aget idxs i)))))
  (-union [_ rel]
    (assert (instance? ArrayRelation rel))
    (assert (= offset-map (:offset-map rel)))
    (ArrayRelation. offset-map (concat coll (:coll rel)))))

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
  (-size    [_]        (count coll)) ;; (if (instance? Iter coll) (btset/est-count coll) (count coll))
  (-getter  [_ symbol]
    (let [idx (offset-map symbol)]
      (fn [tuple]
        (nth tuple idx))))
  (-indexes [_ syms]
    (mapa offset-map syms))
  (-copy-tuple [_ tuple idxs target target-idxs]
    (dotimes [i (da/alength idxs)]
      (da/aset target (da/aget target-idxs i) (nth tuple (da/aget idxs i)))))
  (-union [_ rel]
    (assert (instance? CollRelation rel))
    (assert (= offset-map (:offset-map rel)))
    (CollRelation. offset-map (concat coll (:coll rel)))))

(defn coll-rel [symbols coll]
  (let [offset-map (reduce-kv
                     (fn [acc e i]
                       (cond
                         (instance? BindScalar e)
                           (assoc acc (get-in e [:variable :symbol]) i)
                         (instance? Variable e)
                           (assoc acc (:symbol e) i)
                         (and (symbol? e)
                              (not= '_ e))
                           (assoc acc e i)
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
;;                       (f acc (da/array t1 t2)))
;;                     acc))
;;             init))

;;   (-size [_]
;;     (* (-size rel1) (-size rel2)))
;;   (-getter [_ symbol]
;;     (if (some #{symbol} (-symbols rel1))
;;       (let [getter (-getter rel1 symbol)]
;;         (fn [tuple]
;;           (getter (da/aget tuple 0))))
;;       (let [getter (-getter rel2 symbol)]
;;         (fn [tuple]
;;           (getter (da/aget tuple 1))))))
;;   (-indexes [_ syms]
;;     (let [[syms1 syms2] (split-with (set (-symbols rel1)) syms)]
;;       [(-indexes rel1 syms1)
;;        (count syms1)
;;        (-indexes rel2 syms2)
;;        (count syms2)]))
;;   (-copy-tuple [_ tuple idxs target target-idxs]
;;     (let [[idxs1 arity1 idxs2 arity2] idxs
;;           target-idxs1 (subarr target-idxs 0 arity1)
;;           target-idxs2 (subarr target-idxs arity1 (da/alength target-idxs))]
;;       (-copy-tuple rel1 (da/aget tuple 0) idxs1 target target-idxs1)
;;       (-copy-tuple rel2 (da/aget tuple 1) idxs2 target target-idxs2))))

;; (def prod-rel ->ProdRelation)


;;; SingletonRelation

(deftype SingletonRelation []
  IRelation
  (-symbols    [_] [])
  (-arity      [_] 0)
  (-fold       [_ f init] (f init (to-array [])))
  (-size       [_] 1)
  (-indexes    [_ _] (to-array []))
  (-copy-tuple [_ _ _ _ _]))

(def singleton-rel ->SingletonRelation)

;;; cartesian product

(defn- join-tuples [rel1 t1 idxs1
                    rel2 t2 idxs2
                    arity
                    target-idxs1 target-idxs2]
  (let [arr (da/make-array arity)]
    (-copy-tuple rel1 t1 idxs1 arr target-idxs1)
    (-copy-tuple rel2 t2 idxs2 arr target-idxs2)
    arr))

(defn product [rel1 rel2]
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
      (persistent! coll))))


(defn product-all [rels]
  (reduce product rels)) ;; TODO check for empty rels


;; hash-join


(defn- key-fn [rel syms]
  (let [arity (count syms)]
    (if (== arity 1)
      (-getter rel (first syms))
      (let [idxs        (-indexes rel syms)
            target-idxs (arange 0 arity)]
        (fn [t]
          (let [arr (da/make-array arity)]
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


;; Bindings

(defn- bind! [tuples binding source indexes]
  (condp instance? binding

    BindIgnore
      tuples

    BindScalar
      (let [symbol (get-in binding [:variable :symbol])
            idx    (get indexes symbol)]
        (run! #(da/aset % idx source) tuples)
        tuples)

    BindColl
      (if (not (db/seqable? source))
        (db/raise "Cannot bind value " source " to collection " (dp/source binding)
                  {:error :query/binding, :value source, :binding (dp/source binding)})
        (let [inner-binding (:binding binding)]
          (case (count source)
            0 []
            1 (bind! tuples inner-binding (first source) indexes)
              (into [] ;; TODO fast-arr
                (comp (map #(bind! tuples inner-binding % indexes))
                      cat
                      (map da/aclone))
                source))))

    BindTuple
    (let [bindings (:bindings binding)]
      (when-not (db/seqable? source)
        (db/raise "Cannot bind value " source " to tuple " (dp/source binding)
                  {:error :query/binding, :value source, :binding (dp/source binding)}))
      (when (< (count source) (count bindings))
        (db/raise "Not enough elements in a collection " source " to bind tuple " (dp/source binding)
                  {:error :query/binding, :value source, :binding (dp/source binding)}))
      (reduce (fn [ts [b s]]
                (bind! ts b s indexes))
              tuples
              (zip bindings source)))
    
    :else
      (db/raise "Unknown binding form " (dp/source binding)
               {:error :query/binding, :value source, :binding (dp/source binding)})))


(defn bind [binding source]
  (let [syms    (map :symbol (dp/collect-vars-distinct binding))
        indexes (zipmap syms (range))
        tuples  (bind! [(da/make-array (count syms))] binding source indexes)]
    (array-rel syms tuples)))


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
      (let [rel (bind binding value)]
        (if (== 1 (-size rel))
          (update-in context [:consts] merge (rel->consts rel))
          (update-in context [:rels] conj rel)))))

(defn resolve-ins [context bindings values]
  (when (not= (count bindings) (count values))
    (db/raise "Wrong number of arguments for bindings " (mapv dp/source bindings)
           ", " (count bindings) " required, " (count values) " provided"
           {:error :query/binding, :binding (mapv dp/source bindings)}))
  (reduce resolve-in context (zip bindings values)))


;;; Resolution


(defprotocol IClause
  (-resolve-clause [clause context]))


(defn get-source [context source]
  (let [symbol (cond
                 (instance? SrcVar source)     (:symbol source)
                 (instance? DefaultSrc source) (:default-source-symbol context)
                 :else (db/raise "Source expected, got " source))]
    (or (get (:sources context) symbol)
        (db/raise "Source " symbol " is not defined"
               {:error :query/where, :symbol symbol}))))
    

;; Patterns

(defn resolve-pattern-db [db clause]
  ;; TODO optimize with bound attrs min/max values here
  (let [pattern        (:pattern clause)
        search-pattern (mapv #(when (instance? Constant %) (:value %)) pattern)
        datoms         (db/-search db search-pattern)]
    (coll-rel (:pattern clause) datoms)))


(defn- matches-pattern? [idxs tuple] ;; TODO handle repeated vars
;;   (when-not (db/seqable? tuple)
;;     (db/raise "Cannot match pattern " (dp/source clause) " because tuple is not a collection: " tuple
;;            {:error :query/where, :value tuple, :binding (dp/source clause)}))
;;   (when (< (count tuple) (count (:pattern clause)))
;;     (db/raise "Not enough elements in a relation tuple " tuple " to match " (dp/source clause)
;;            {:error :query/where, :value tuple, :binding (dp/source clause)}))
  (reduce-kv
    (fn [_ i v]
      (if (not= (nth tuple i) v) ;; nth?
        (reduced false)
        true))
    true
    idxs))


(defn resolve-pattern-coll [coll clause]
  (when-not (db/seqable? coll)
    (db/raise "Cannot match by pattern " (dp/source clause) " because source is not a collection: " coll
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
      (dp/postwalk
        clause
        (fn [form]
          (if (instance? Variable form)
            (let [sym (:symbol form)]
              (if-let [subs (get (:consts context) (:symbol form))]
                (Constant. subs)
                form))
            form)))
      clause)))

(defn related-rels [context syms]
  (let [syms (set syms)]
    (->> (:rels context)
         (filter #(some syms (-symbols %))))))


(defn extract-rels [context syms]
  (let [syms      (set syms)
        rels      (:rels context)
        related?  #(some syms (-symbols %))
        related   (filter related? rels)]
    (if (empty? related)
      [nil context]
      (let [unrelated (remove related? rels)]
        [related (assoc context :rels unrelated)]))))


(defn join-unrelated [context rel]
  (case (long (-size rel))
    0 empty-context
    1 (update context :consts merge (rel->consts rel))
    (update context :rels conj rel)))


(defn hash-join-rel [context rel]
  (if (== 0 (-size rel))
    empty-context
    (let [syms                    (set (-symbols rel))
          [related-rels context*] (extract-rels context syms)]
      (if (empty? related-rels)
        (join-unrelated context rel)
        (let [related-rel (product-all related-rels)
              join-syms   (set/intersection syms (set (-symbols related-rel)))
              hash        (hash-map-rel related-rel join-syms)
              ;; TODO choose between hash-join and lookup-join
              rel*        (hash-join related-rel hash join-syms rel)]
          (join-unrelated context* rel*))))))


(defn resolve-pattern [context clause]
  (let [clause* (substitute-constants clause context)
        rel     (let [source (get-source context (:source clause))]
                  (if (satisfies? db/ISearch source)
                    (resolve-pattern-db   source clause*)
                    (resolve-pattern-coll source clause*)))]
    (hash-join-rel context rel)))


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
    (collect-to context syms (fast-set) [(map vec)])))


(defn subtract-from-rel [rel syms exclude-key-set]
  (let [key-fn1 (key-fn rel syms)
        pred    (fn [t1] (contains? exclude-key-set (key-fn1 t1)))]
    (-alter-coll rel #(into (fast-arr) (remove pred) %))))


(defn subtract-contexts [context1 context2 syms]
  (if (:empty? context2) ;; empty context2 means there’s nothing to subtract
    context1
    ;; ignoring constants inherited from context1
    (let [syms* (set/difference syms (keys (:consts context1)))]
      (if (empty? syms*) ;; join happened by constants only
        empty-context    ;; context2 is not-empty, meaning it satisfied constans,
                         ;; meaning we proved constants in context1 do not match
        (let [[rels1 context1*] (extract-rels context1 syms*)
              rel1              (product-all rels1)
              set2              (collect-opt context2 syms*)
              rel1*             (subtract-from-rel rel1 syms* set2)]
          (join-unrelated context1* rel1*))))))


(defn upd-default-source [context clause]
  (let [source (:source clause)]
    (if (instance? SrcVar source)
      (assoc context :default-source-symbol (:symbol source))
      context)))


(defn check-bound [context syms form]
  (let [context-syms (-> #{}
                         (into (keys (:consts context)))
                         (into (mapcat -symbols) (:rels context))
                         (into (keys (:sources context))))]
    (when-not (set/subset? syms context-syms)
      (let [missing (set/difference (set syms) context-syms)]
        (throw (ex-info (str "Insufficient bindings: " missing " not bound in " form)
                        {:error :query/where
                         :form  form
                         :vars  missing}))))))
                           

(defn resolve-not [context clause]
  (let [{:keys [source vars clauses]} clause
        syms      (into #{} (map :symbol) vars)
        _         (check-bound context syms (dp/source clause))
        context*  (-> context
                    (project-context syms) ;; sub-context will only see symbols Not is joined by
                    (upd-default-source clause)
                    (resolve-clauses clauses))]
    (subtract-contexts context context* syms)))


(defn resolve-or [context clause]
  (let [{:keys [source rule-vars clauses]} clause
        {:keys [required free]}            rule-vars
        _    (check-bound context (map :symbol required) (dp/source clause))
        syms (into #{} (map :symbol) (concat required free))
        context*  (-> context
                    (project-context syms)
                    (upd-default-source clause))
        contexts  (->> clauses
                        (map #(-resolve-clause % context*))
                        (remove :empty?))]
    (if (empty? contexts)
      empty-context ;; everything resolved to empty rel, short-circuit
      (let [non-consts (set/difference syms (set (keys (:consts context))))]
        (if (empty? non-consts)
          context ;; join was by constants only, nothing changes
          (let [arrays (map #(collect-to % non-consts (fast-arr)) contexts)
                rel    (array-rel non-consts (into (first arrays) cat (next arrays)))]
            (hash-join-rel context rel)))))))


(defn collect-args! [context args target form]
  (let [consts  (:consts context)
        sources (:sources context)]
    (doseq [[arg i] (zip args (range))
            :let [sym (:symbol arg)]]
      (cond
        (instance? Variable arg)
          (when (contains? consts sym)
            (da/aset target i (get consts sym)))
        (instance? SrcVar arg)
          (if (contains? sources sym)
            (da/aset target i (get sources sym))
            (throw (ex-info (str "Unbound source variable: " sym " in " form)
                            { :error :query/where, :form form, :var sym })))
        (instance? Constant arg)
          (da/aset target i (:value arg))))))


(defn get-f [context fun form]
  (let [sym (:symbol fun)]
    (if (instance? PlainSymbol fun)
      (or (get dq/built-ins sym)
          (throw (ex-info (str "Unknown built-in " sym " in " form)
                          {:error :query/where, :form form, :var sym})))
      (or (get (:consts context) sym) ;; variable then
          (throw (ex-info (str "Unknown function " sym " in " form)
                          {:error :query/where, :form form, :var sym}))))))


(defn resolve-predicate [context clause]
  (let [{fun :fn, args :args} clause
        form      (dp/source clause)
        f         (get-f context fun form)
        args-arr  (da/make-array (count args))
        _         (collect-args! context args args-arr form)
        consts    (:consts context)
        sym+idx   (for [[arg i] (zip args (range))
                        :when   (instance? Variable arg)
                        :let    [sym (:symbol arg)]
                        :when   (not (contains? consts sym))]
                    [sym i])
        args-syms (map first sym+idx)
        args-idxs (mapa second sym+idx)
        _         (check-bound context args-syms form)]
    (if (empty? args-syms) ;; only constants
      (if (apply f (vec args-arr))
        context
        empty-context)
      (let [[rels context*] (extract-rels context args-syms)]
        (if (== 1 (count rels))
          (let [rel  (first rels)
                idxs (-indexes rel args-syms)
                pred (fn [tuple]
                        (-copy-tuple rel tuple idxs args-arr args-idxs)
                        (apply f (vec args-arr)))
                rel* (-alter-coll rel #(filterv pred %))]
            (join-unrelated context* rel*))
          (let [prod-syms    (mapcat -symbols rels)
                prod-sym+idx (zip prod-syms (range))
                xfs          (map #(collect-rel-xf prod-sym+idx %) rels)
                prod-rel     (array-rel prod-syms [])

                idxs         (-indexes prod-rel args-syms)
                pred         (fn [tuple]
                                (-copy-tuple prod-rel tuple idxs args-arr args-idxs)
                                (apply f (vec args-arr)))

                array        (into (fast-arr)
                                (apply comp (concat xfs [(filter pred)]))
                                [(da/make-array (count prod-syms))])
                prod-rel*    (array-rel prod-syms array)]
        (join-unrelated context* prod-rel*)))))))


(extend-protocol IClause
  Pattern
  (-resolve-clause [clause context]
    (resolve-pattern context clause))
  Not
  (-resolve-clause [clause context]
    (resolve-not context clause))
  Or
  (-resolve-clause [clause context]
    (resolve-or context clause))
  And
  (-resolve-clause [clause context]
    (resolve-clauses context (:clauses clause)))
  Predicate
  (-resolve-clause [clause context]
    (resolve-predicate context clause)))


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
        (da/aset specimen i val)))))

        
(defn collect-rel-xf [syms-indexed rel]
  (let [sym+idx     (for [[sym i] syms-indexed
                          :when (has? (-symbols rel) sym)]
                      [sym i])
        idxs        (-indexes rel (map first sym+idx))
        target-idxs (mapa second sym+idx)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result specimen]
          (-fold rel
            (fn [acc tuple]
              (let [t (da/aclone specimen)]
                (-copy-tuple rel tuple idxs t target-idxs)
                (rf acc t)))
            result))))))


(defn collect-to
  ([context syms acc]
   (collect-to context syms acc [] (da/make-array (count syms))))
  ([context syms acc xfs]
   (collect-to context syms acc xfs (da/make-array (count syms))))
  ([context syms acc xfs specimen]
    ;; TODO don't collect if array-rel and matches symbols
    (if (:empty? context)
      acc
      (let [syms-indexed (vec (zip syms (range)))
            _            (collect-consts syms-indexed specimen (:consts context))
            related-rels (related-rels context syms)
            xfs          (-> (map #(collect-rel-xf syms-indexed %) related-rels)
                              (concat xfs))]
        (into acc (apply comp xfs) [specimen])))))


;; Query


(def query-cache (volatile! (datascript.lru/lru lru-cache-size)))


(defn parse-query [q]
  (if-let [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))


(defn q [q & inputs]
  (let [parsed-q (parse-query q)
        context  { :rels    []
                    :consts  {}
                    :sources {}
                    :rules   {}
                    :default-source-symbol '$ }
        context  (resolve-ins context (:qin parsed-q) inputs)
        context  (resolve-clauses context (:qwhere parsed-q))
        syms     (concat (dp/find-vars (:qfind parsed-q))
                          (map :symbol (:qwith parsed-q)))]
    (native-coll (collect-to context syms (fast-set) [(map vec)]))))


(comment
  (t/test-ns 'datascript.test.query-v3)

  (let [query   '[ :find  ?lid ?status ?starttime ?endtime (min ?paid) (distinct ?studentinfo) ?lgid
                  :in    $ ?tid ?week ?list
                  :where [?lid :lesson/teacherid ?tid]
                          [?lid :lesson/week ?week]
                          [?lid :lesson/lessongroupid ?lgid]
                          [?eid :enrollment/lessongroup_id ?lgid]
                          [?eid :enrollment/student_id ?sid]
                          [?iid :invoice/enrollment_id ?eid]
                          [?sid :student/firstname ?fname]
                          [?sid :student/lastname ?lname]
                          [?iid :invoice/paid ?paid]
                          [?lid :lesson/status ?status]
                          [?lid :lesson/starttime ?starttime]
                          [?lid :lesson/endtime ?endtime]
                          [(?list ?sid ?fname ?lname) ?studentinfo]]
        parsed (dp/parse-query query)]
    (perf/minibench "postwalk"
      (dp/postwalk parsed identity))
    (perf/minibench "parse-query"
      (dp/parse-query query))
    (perf/minibench "substitute-constants"
      (substitute-constants (first (:where parsed)) {:consts {'?tid 7}})))

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
