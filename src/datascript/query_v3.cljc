(ns datascript.query-v3
  (:require
    [clojure.set :as set]
    ;;[cemerick.cljs.test :as t]
    [datascript :as d]
    [datascript.core :as dc]
    [datascript.parser :as dp #?@(:cljs [:refer [BindColl BindIgnore BindScalar BindTuple
                                                 Constant DefaultSrc Pattern RulesVar SrcVar Variable]])]
    [datascript.btset :as btset]
    [datascript.perf :as perf])
  #?(:clj (:import [datascript.parser BindColl BindIgnore BindScalar BindTuple
                                      Constant DefaultSrc Pattern RulesVar SrcVar Variable])))

(defn mapa [f coll]
  #?(:cljs
     ;; perf optimization, instead of using into-array as in CLJ
     (let [res (js/Array.)]
       (doseq [el coll]
         (.push res (f el)))
       res)
     :clj
     (into-array (map f coll))))

(defn concatv [& xs]
  (into [] cat xs))

;; (defrecord Context [rels consts zeroes sources rules default-source-symbol])

(defprotocol IRelation
  (-getter [_ symbol])
  (-indexes [_ symbols])
  (-copy-tuple [_ tuple idxs target offset])
  (-union [_ rel]))

(defn- first-tuple [rel]
  (reduce (fn [_ tuple] (reduced tuple)) nil rel))

(defn- #?@(:clj  [^Number hash-arr]
           :cljs [^number hash-arr]) [arr]
  (loop [n 0
         hash-code 1]
    (if (< n (alength arr))
      (recur (inc n) (bit-or (+ (#?(:cljs imul :clj unchecked-multiply) 31 hash-code) (hash (aget arr n))) 0))
      (mix-collection-hash hash-code n))))

(deftype Tuple [arr _hash]
  #?@(:cljs
       [Object
        (equiv [this other]
               (-equiv this other))

        IHash
        (-hash [_] _hash)

        IEquiv
        (-equiv [_ other]
                (boolean
                 (and
                  (== (alength arr)
                      (alength (.-arr other)))
                  (loop [i 0]
                    (cond
                      (== i (alength arr)) true
                      (not= (aget arr i) (aget (.-arr other) i)) false
                      :else (recur (inc i)))))))]))

(defn tuple [arr]
  (Tuple. arr (hash-arr arr)))

;;; ArrayRelation

(deftype ArrayRelation [symbols arity coll]
  #?@(:cljs
      [Object
       (toString [this] (pr-str* this))



       IReduce
       (-reduce [_ f init]
                (reduce f init coll))

       ICounted
       (-count [_] (count coll))

       IRelation
       (-getter [_ symbol]
                (let [idx (get (zipmap symbols (range)) symbol)]
                  (fn [tuple]
                    (aget tuple idx))))
       (-indexes [_ syms]
                 (mapa (zipmap symbols (range)) syms))
       (-copy-tuple [_ tuple idxs target offset]
                    (dotimes [i (alength idxs)]
                      (aset target (+ i offset) (aget tuple (aget idxs i)))))
       (-union [_ rel]
               (ArrayRelation. symbols arity (into coll (.-coll rel))))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-write writer "#ArrayRelation")
                   (pr-writer {:symbols symbols, :arity arity, :coll coll} writer opts))]))

(defn array-rel [symbols coll]
  (ArrayRelation. symbols (count symbols) coll))

;;; Coll1Relation

(deftype Coll1Relation [symbols arity coll]
  #?@(:cljs
      [IReduce
       (-reduce [_ f init]
                (reduce f init coll))

       ICounted
       (-count [_] (count coll))

       IRelation
       (-getter [_ _]
                (fn [el] el))
       (-indexes [_ syms] 0)
       (-copy-tuple [_ el _ target offset]
                    (aset target offset el))
       (-union [_ rel]
               (Coll1Relation. symbols arity (into coll (.-coll rel))))

       Object
       (toString [this] (pr-str* this))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-write writer "#Coll1Relation")
                   (pr-writer {:symbols symbols, :arity arity, :coll coll} writer opts))]))

(defn coll1-rel [symbol coll]
  (Coll1Relation. [symbol] 1 coll))

;;; CollRelation

(deftype CollRelation [symbols offset-map arity coll]
  #?@(:cljs
      [IReduce
       (-reduce [_ f init]
                (reduce f init coll))

       ICounted
       (-count [_] (count coll))

       IRelation
       (-getter [_ symbol]
                (let [idx (offset-map symbol)]
                  (fn [tuple]
                    (nth tuple idx))))
       (-indexes [_ syms]
                 (mapa offset-map syms))
       (-copy-tuple [_ tuple idxs target offset]
                    (dotimes [i (alength idxs)]
                      (aset target (+ i offset) (nth tuple (aget idxs i)))))
       (-union [_ rel]
               (CollRelation. symbols offset-map arity (into coll (.-coll rel))))

       Object
       (toString [this] (pr-str* this))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-write writer "#CollRelation")
                   (pr-writer {:symbols symbols, :offset-map offset-map, :arity arity, :coll coll} writer opts))]))

(defn coll-rel [symbols coll]
  (let [offset-map (reduce-kv
                     (fn [acc s i]
                       (if (not= s '_)
                         (assoc acc s i)
                         acc))
                     {}
                     (zipmap symbols (range)))]
    (CollRelation. (keys offset-map) offset-map (count offset-map) coll)))


;;; ProdRelation

(deftype ProdRelation [symbols arity rel1 rel2]  
  #?@(:cljs
      [IReduce
       (-reduce [_ f init]
                (reduce (fn [acc t1]
                          (reduce (fn [acc t2]
                                    (f acc (dc/into-arr [t1 t2])))
                                  acc rel2))
                        init rel1))

       ICounted
       (-count [_] (* (-count rel1) (-count rel2)))

       IRelation
       (-getter [_ symbol]
                (if (some #{symbol} (.-symbols rel1))
                  (-getter rel1 symbol)
                  (-getter rel2 symbol)))
       (-indexes [_ syms]
                 (let [[syms1 syms2] (split-with (set (.-symbols rel1)) syms)]
                   (dc/into-arr [(-indexes rel1 syms1) (-indexes rel2 syms2)])))
       (-copy-tuple [_ tuple idxs target offset]
                    (let [idxs1 (aget idxs 0)
                          idxs2 (aget idxs 1)]
                      (-copy-tuple rel1 (aget tuple 0) idxs1 target offset)
                      (-copy-tuple rel2 (aget tuple 1) idxs2 target (+ offset (.-arity rel1)))))

       Object
       (toString [this] (pr-str* this))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-write writer "#ProdRelation")
                   (pr-writer {:rel1 rel1, :rel2 rel2} writer opts))]))

(defn prod-rel [rel1 rel2]
  (ProdRelation.
    (concatv (.-symbols rel1) (.-symbols rel2))
    (+ (.-arity rel1) (.-arity rel2))
    rel1
    rel2))


;;; EmptyRelation

(deftype EmptyRelation [symbols arity]
  #?@(:cljs
       [IReduce
        (-reduce [_ f init] init)

        ICounted
        (-count [_] 0)

        IRelation
        (-indexes [_ syms]
                  (mapa (zipmap symbols (range)) syms))

        Object
        (toString [this] (pr-str* this))

        IPrintWithWriter
        (-pr-writer [_ writer opts]
                    (-write writer "#EmptyRelation")
                    (pr-writer {:symbols symbols, :arity arity} writer opts))]))

(defn- empty-rel [symbols]
  (EmptyRelation. symbols (count symbols)))

;;; SingletonRelation

(deftype SingletonRelation [symbols arity]
  #?@(:cljs
       [IReduce
        (-reduce [_ f init]
                 (f init (dc/into-arr [])))

        ICounted
        (-count [_] 1)

        IRelation
        (-indexes [_ _] (dc/into-arr []))
        (-copy-tuple [_ _ _ _ _])

        Object
        (toString [this] (pr-str* this))

        IPrintWithWriter
        (-pr-writer [_ writer opts]
                    (-write writer "#SingletonRelation")
                    (pr-writer {:symbols symbols, :arity arity} writer opts))]))

(defn singleton-rel []
  (SingletonRelation. [] 0))

;;; cartesian product

(defn- join-tuples [rel1 t1 idxs1
                   rel2 t2 idxs2
                   arity offset]
  (let [arr (make-array arity)]
    (-copy-tuple rel1 t1 idxs1 arr 0)
    (-copy-tuple rel2 t2 idxs2 arr offset)
    arr))

(defn product [rel1 rel2]
  (let [idxs1  (-indexes rel1 (.-symbols rel1))
        idxs2  (-indexes rel2 (.-symbols rel2))
        arity  (+ (.-arity rel1) (.-arity rel2))
        offset (.-arity rel1)
        coll   (reduce
                 (fn [acc t1]
                   (reduce
                     (fn [acc t2]
                       (conj! acc (join-tuples rel1 t1 idxs1
                                               rel2 t2 idxs2
                                               arity offset)))
                    acc
                    rel2))
                 (transient [])
                 rel1)]
    (array-rel
      (concatv (.-symbols rel1) (.-symbols rel2))
      (persistent! coll))))


;;; hash-join

(defn- key-fn [rel syms]
  (let [arity (count syms)]
    (if (== arity 1)
      (-getter rel (first syms))
      (let [idxs (-indexes rel syms)]
        (fn [t]
          (let [arr (make-array arity)]
            (-copy-tuple rel t idxs arr 0)
            (tuple arr)))))))

(defn- common-symbols [rel1 rel2]
  (filter (set (.-symbols rel2)) (.-symbols rel1)))

(defn hash-rel [rel syms]
  (let [key-fn (key-fn rel syms)]
    (reduce
      (fn [hash t]
        (let [key (key-fn t)
              old (get hash key nil)]
          (if (nil? old)
            (assoc! hash key (dc/into-arr [t]))
            (do (.push old t) hash))))
      (transient {})
      rel)))

(defn hash-join [rel1 hash1 join-syms rel2]
  (let [syms1       (.-symbols rel1)
        syms2       (.-symbols rel2)
        keep-syms2  (remove (set syms1) syms2)
        key-fn2     (key-fn rel2 join-syms)
        idxs1       (-indexes rel1 syms1)
        idxs2       (-indexes rel2 keep-syms2)
        full-syms   (concatv syms1 keep-syms2)
        full-arity  (count full-syms)
        offset      (.-arity rel1)
        
        coll        (reduce ;; iterate over rel2
                      (fn [acc t2]
                        (let [tuples1 (get hash1 (key-fn2 t2) nil)]
                          (if (nil? tuples1)
                            acc
                            (areduce tuples1 i acc acc
                                     (let [t1 (aget tuples1 i)]
                                       (conj! acc (join-tuples rel1 t1 idxs1
                                                               rel2 t2 idxs2
                                                               full-arity offset)))))))
                      (transient [])
                      rel2)]
    (array-rel full-syms (persistent! coll))))


;;; Union

(defn union [rel1 rel2]
  (assert (= (type rel1) (type rel2)))
  (-union rel1 rel2))

;;; Bindings

(defn- bindable-to-seq? [x]
  (or (dc/seqable? x) (dc/array? x)))

(defn- binding-symbols [binding]
  (->> (dp/collect-vars-distinct binding)
       (map :symbol)))

(defn- plain-tuple-binding? [binding]
  (and (instance? BindTuple binding)
       (every? #(or (instance? BindScalar %)
                    (instance? BindIgnore %))
               (.-bindings binding))))

(defn- bind-tuples [bindings coll]
  (coll-rel (mapv #(if (instance? BindScalar %)
                     (.. % -variable -symbol)
                     '_)
              bindings)
    coll))

(defprotocol IBinding
  (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (singleton-rel))
  
  BindScalar
  (in->rel [binding value]
    (array-rel [(.. binding -variable -symbol)] [(dc/into-arr [value])]))
  
  BindColl
  (in->rel [binding coll]
    (let [inner-binding (.-binding binding)]
      (cond
        (not (bindable-to-seq? coll))
          (dc/raise "Cannot bind value " coll " to collection " (dp/source binding)
                 {:error :query/binding, :value coll, :binding (dp/source binding)})
       
        (empty? coll)
          (empty-rel (binding-symbols binding))
       
        ;; [?x ...] type of binding
        (instance? BindScalar inner-binding)
          (coll1-rel (.. inner-binding -variable -symbol) coll)
       
        ;; [[?x ?y] ...] type of binding
        (plain-tuple-binding? inner-binding)
          (bind-tuples (.-bindings inner-binding) coll)

        ;; something more complex, fallback to generic case
        :else
          (reduce union
            (map #(in->rel inner-binding %) coll)))))
  
  BindTuple
  (in->rel [binding coll]
    (cond
      (not (bindable-to-seq? coll))
        (dc/raise "Cannot bind value " coll " to tuple " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      (< (count coll) (count (.-bindings binding)))
        (dc/raise "Not enough elements in a collection " coll " to bind tuple " (dp/source binding)
               {:error :query/binding, :value coll, :binding (dp/source binding)})
      ;; [?x ?y] type of binding
      (plain-tuple-binding? binding)
        (bind-tuples (.-bindings binding) [coll])
      ;; fallback to generic case
      :else
        (reduce product
          (map #(in->rel %1 %2) (.-bindings binding) coll)))))

(defn- rel->consts [rel]
  {:pre [(== (count rel) 1)]}
  (into {} (map vector (.-symbols rel) (first-tuple rel))))

(defn- resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (.-variable binding)))
      (update-in context [:sources] assoc (.. binding -variable -symbol) value)
;;     (and (instance? BindScalar binding)
;;          (instance? RulesVar (.-variable binding)))
;;       (assoc context :rules (parse-rules value))
    :else
      (let [rel (in->rel binding value)]
        (if (== 1 (count rel))
          (update-in context [:consts] merge (rel->consts rel))
          (update-in context [:rels] conj rel)))))

(defn resolve-ins [context bindings values]
  (when (not= (count bindings) (count values))
    (dc/raise "Wrong number of arguments for bindings " (mapv dp/source bindings)
           ", " (count bindings) " required, " (count values) " provided"
           {:error :query/binding, :binding (mapv dp/source bindings)}))
  (reduce resolve-in context (map vector bindings values)))

;;; Resolution

(defn substitute-constants [clause context]
  (perf/measure
    (dp/postwalk
      clause
      (fn [form]
        (if (instance? Variable form)
          (let [sym (.-symbol form)]
            (if-let [subs ((:consts context) (.-symbol form))]
              (do
                (perf/debug "substituted" sym "with" subs)
                (Constant. subs)
              )
              form))
          form)))
   "substitute-constants"))

(defprotocol IClause
  (-resolve-clause [_ context join-args hash]))

(defn get-source [context source]
  (let [symbol (cond
                 (instance? SrcVar source)     (.-symbol source)
                 (instance? DefaultSrc source) (:default-source-symbol context)
                 :else (dc/raise "Source expected, got " source))]
    (or (get (:sources context) symbol)
        (dc/raise "Source " symbol " is not defined"
               {:error :query/where, :symbol symbol}))))
    

;; Patterns

(deftype DatomsRelation [symbols props-map arity iter]
  #?@(:cljs
      [IReduce
       (-reduce [_ f init]
                (reduce f init iter))

       ICounted
       (-count [_] (btset/est-count iter))

       IRelation
       (-getter [_ symbol]
                (let [prop (props-map symbol)]
                  (fn [tuple]
                    (aget tuple prop))))
       (-indexes [_ syms]
                 (mapa props-map syms))
       (-copy-tuple [_ tuple props target offset]
                    (dotimes [i (alength props)]
                      (aset target (+ i offset) (aget tuple (aget props i)))))
       ;;   (-union [_ rel]
       ;;     (CollRelation. symbols offsets arity (into coll (.-coll rel))))

       Object
       (toString [this] (pr-str* this))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-write writer "#IndexRelation")
                   (pr-writer {:symbols symbols, :props-map props-map, :arity arity, :iter iter} writer opts))]))

(defn- pattern->props-map [pattern]
  (reduce-kv
    (fn [acc var prop]
      (if (instance? Variable var)
        (assoc acc (.-symbol var) prop)
        acc))
    {}
    (zipmap pattern ["e" "a" "v" "tx"])))

(defn -resolve-pattern-db [db clause join-syms hash]
  ;; TODO optimize with bound attrs min/max values here
  (let [pattern        (.-pattern clause)
        search-pattern (mapv #(when (instance? Constant %) (.-value %)) pattern)
        datoms         (dc/-search db search-pattern)
        props-map      (pattern->props-map (.-pattern clause))]
    (DatomsRelation. (keys props-map) props-map (count props-map) datoms)))

(defn- matches-pattern? [idxs tuple]
;;   (when-not (bindable-to-seq? tuple)
;;     (dc/raise "Cannot match pattern " (dp/source clause) " because tuple is not a collection: " tuple
;;            {:error :query/where, :value tuple, :binding (dp/source clause)}))
;;   (when (< (count tuple) (count (.-pattern clause)))
;;     (dc/raise "Not enough elements in a relation tuple " tuple " to match " (dp/source clause)
;;            {:error :query/where, :value tuple, :binding (dp/source clause)}))
  (reduce-kv
    (fn [_ i v]
      (if (not= (nth tuple i) v)
        (reduced false)
        true))
    true
    idxs))

(defn- bind-pattern [pattern coll]
  ;; TODO handle repeated vars
  (coll-rel (mapv #(if (instance? Variable %)
                     (.-symbol %)
                     '_)
              pattern)
    coll))

(defn -resolve-pattern-coll [coll clause]
  (when-not (bindable-to-seq? coll)
    (dc/raise "Cannot match by pattern " (dp/source clause) " because source is not a collection: " coll
       {:error :query/where, :value coll, :binding (dp/source clause)}))
  (let [pattern (.-pattern clause)
        idxs    (->> (map #(when (instance? Constant %1) [%2 (.-value %1)]) pattern (range))
                     (remove nil?)
                     (into {}))
        data    (filter #(matches-pattern? idxs %) coll)]
    (bind-pattern pattern data)))

(extend-protocol IClause
  Pattern
  (-resolve-clause [clause context join-syms hash]
    (let [source (get-source context (.-source clause))]
      (if (satisfies? dc/ISearch source)
        (-resolve-pattern-db   source clause join-syms hash)
        (-resolve-pattern-coll source clause)))))

(defn resolve-clause-new [context clause]
  (perf/measure (-resolve-clause clause context nil nil)
     "resolve-clause-new to" (.-symbols %) "with" (count %) "tuples:" %))

(defn resolve-clause-related [context clause clause-syms related-rels]
  (perf/measure
    (let [related   (reduce product related-rels)
          join-syms (filter clause-syms (.-symbols related))
          _         (perf/debug "got" (count related-rels) "related rels over" join-syms)
          _         (perf/when-debug
                      (doseq [rel related-rels]
                        (perf/debug "  " (.-symbols rel) "with" (count rel) "tuples")))
          hash      (perf/measure (hash-rel related join-syms)
                             "hash calculated with" (count %) "keys")
          rel       (perf/measure (-resolve-clause clause context join-syms hash) ;; TODO use hash
                             "-resolve-clause to" (count %) "tuples")
          joined    (perf/measure (hash-join related hash join-syms rel) ;; TODO choose between hash-join and lookup-join
                             "hash-join to" (.-symbols %) "with" (count %) "tuples")]
      joined)
    "resolve-clause-related"))

(defn resolve-clause [context clause]
  (perf/measure
    (let [clause-syms  (perf/measure (into #{} (map :symbol) (dp/collect #(instance? Variable %) clause #{})) "clause-syms")
          consts       (into #{} (filter (:consts context)) clause-syms)
          old-rels     (:rels context)
          related?     #(some clause-syms (.-symbols %))
          related-rels (filter related? old-rels)
          clause*      (if (some (:consts context) clause-syms)
                         (substitute-constants clause context)
                         clause)
          new-rel      (cond
                         (some (:empties context) clause-syms)
                           (empty-rel (vec (set (concat clause-syms (mapcat #(.-symbols %) related-rels)))))
                         (empty? related-rels)
                           (resolve-clause-new context clause*)
                         :else
                           (resolve-clause-related context clause* clause-syms related-rels))
          keep-rels    (remove related? old-rels)
          cardinality  (count new-rel)]
      (cond
        (== 0 cardinality)
          (do
            (perf/debug "Promoting to :empties" (set/union clause-syms consts))
            (-> context
              (update :consts #(apply dissoc % consts))
              (update :empties set/union clause-syms consts)))
        (== 1 cardinality)
          (do
            (perf/debug "Promoting to :consts" (rel->consts new-rel))
            (-> context
              (update :consts merge (rel->consts new-rel))
              (assoc :rels keep-rels)))
        :else
          (assoc context :rels (conj keep-rels new-rel))))
   "resolve-clause" (dp/source clause)))

;;; Query

(def query-cache (volatile! {}))
(defn parse-query [q]
  (or (@query-cache q)
      (let [parsed-q (dp/parse-query q)]
        (vswap! query-cache assoc q parsed-q) ;; TODO limit cache size
        parsed-q)))

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
          context  (perf/measure (reduce resolve-clause context (:where parsed-q))
                     "resolve-clauses")]
      context)
    "Query" q))

#_(perf/minibench "q coll"
  (q '[:find ?a
       :in $1 $2 ?n
       :where [$1 ?a ?n ?b]
              [$2 ?n ?a]]
     (repeatedly 100 (fn [] [(rand-nth [:a :b :c :d]) (rand-int 10) (rand-nth [:x :y :z])]))
     (repeatedly 10 (fn [] [(rand-int 10) (rand-nth [:a :b :c :d]) ]))
     1))

(defn rand-entity []
  {:name (rand-nth ["ivan" "oleg" "petr" "igor"])
   :age  (rand-int 10)})

;; (def db (d/db-with (d/empty-db) (repeatedly 10000 rand-entity)))

#_(binding [datascript.debug/debug? true]
  (q '[:find ?e
         :in $ ?n
         :where [?e :name "ivan"]
                [?e :age ?n]]
       (d/db-with (d/empty-db) (repeatedly 10 rand-entity))
       1))

#_(perf/minibench "q2 const"
  (d/q '[:find ?e
         :where [?e :name "ivan"]
                [?e :age 1]]
     db))


#_(perf/minibench "q3 db"
  (q '[:find ?e
       :in $ ?n
       :where [?e :name "ivan"]
              [?e :age ?n]]
     db
     1))
  
#_(perf/minibench "q2 db"
  (d/q '[:find ?e
         :in $ ?n
         :where [?e :name "ivan"]
                [?e :age ?n]]
     db
     1))



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



