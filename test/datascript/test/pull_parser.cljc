(ns datascript.test.pull-parser
  (:require
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.pull-parser :as dpp]
    [datascript.test.core :as tdc]))

(def *db
  (delay
    (d/empty-db
      {:ref            {:db/valueType :db.type/ref}
       :ref2           {:db/valueType :db.type/ref}
       :ref3           {:db/valueType :db.type/ref}
       :ns/ref         {:db/valueType :db.type/ref}
       :multival       {:db/cardinality :db.cardinality/many}
       :multiref       {:db/valueType :db.type/ref
                        :db/cardinality :db.cardinality/many}
       :component      {:db/valueType :db.type/ref
                        :db/isComponent true}
       :multicomponent {:db/valueType :db.type/ref
                        :db/isComponent true
                        :db/cardinality :db.cardinality/many}})))

(defn pattern [& {:as args}]
  (let [attrs (filter #(not= :db/id (:name %)) (:attrs args))]
    (dpp/map->PullPattern (merge {:first-attr (first attrs) :last-attr (last attrs)} args))))

(defn attr [name & {:as args}]
  (dpp/map->PullAttr
    (merge
      {:name name :xform identity :as name}
      (when (db/ref? @*db name) {:pattern dpp/default-pattern-ref})
      args)))

(deftest test-parse-pattern
  (are [pattern expected] (= expected (dpp/parse-pattern @*db pattern))
    [:normal]    (pattern :attrs [(attr :normal)])
    ['(:normal)] (pattern :attrs [(attr :normal)])
    [[:normal]]  (pattern :attrs [(attr :normal)])
    [:db/id]     (pattern :attrs [(attr :db/id)])

    ; wildcards
    ['*]         (pattern :attrs [(attr :db/id)] :wildcard? true)
    ["*"]        (pattern :attrs [(attr :db/id)] :wildcard? true)
    ['* :normal] (pattern :attrs [(attr :normal) (attr :db/id)] :wildcard? true)
    ['* :db/id]  (pattern :attrs [(attr :db/id)] :wildcard? true)
    ['* [:db/id :as :xxx]] (pattern :attrs [(attr :db/id :as :xxx)] :wildcard? true)

    ; refs
    [:ref]        (pattern :attrs [(attr :ref, :ref? true)])
    [:_ref]       (pattern :reverse-attrs [(attr :ref, :ref? true, :as :_ref, :reverse? true)])
    [:component]  (pattern :attrs [(attr :component, :ref? true, :component? true, :pattern dpp/default-pattern-component)])
    [:_component] (pattern :reverse-attrs [(attr :component, :ref? true, :component? true, :as :_component, :reverse? true)])

    ; reverse
    [:_ref]    (pattern :reverse-attrs [(attr :ref, :as :_ref, :ref? true, :reverse? true)])
    [:ns/_ref] (pattern :reverse-attrs [(attr :ns/ref, :as :ns/_ref, :ref? true, :reverse? true)])

    ; sorting
    [:c :b :a]            (pattern :attrs [(attr :a) (attr :b) (attr :c)])
    [:ref2 :ref3 :ref]    (pattern :attrs [(attr :ref, :ref? true) (attr :ref2, :ref? true) (attr :ref3, :ref? true)])
    [:_ref2 :_ref3 :_ref] (pattern :reverse-attrs [(attr :ref,  :ref? true, :as :_ref, :reverse? true)
                                                   (attr :ref2, :ref? true, :as :_ref2, :reverse? true)
                                                   (attr :ref3, :ref? true, :as :_ref3, :reverse? true)])
    [:ref2 '(:ref3 :as :ref) '(:ref :as :ref3)] (pattern :attrs [(attr :ref, :ref? true, :as :ref3)
                                                                 (attr :ref2, :ref? true)
                                                                 (attr :ref3, :ref? true, :as :ref)])

    ; as
    ['(:normal :as :normal2)]  (pattern :attrs [(attr :normal :as :normal2)])
    ['(:normal :as "normal2")] (pattern :attrs [(attr :normal :as "normal2")])
    ['(:normal :as 123)]       (pattern :attrs [(attr :normal :as 123)])
    ['(:normal :as nil)]       (pattern :attrs [(attr :normal :as nil)])
    ['(:ns/_ref :as :ns/ref)]  (pattern :reverse-attrs [(attr :ns/ref, :as :ns/ref, :ref? true, :reverse? true)])
    ['(:db/id :as :id)]        (pattern :attrs [(attr :db/id :as :id)])

    ; limit
    [:multival]                (pattern :attrs [(attr :multival, :multival? true, :limit 1000)])
    ['(:multival :limit 100)]  (pattern :attrs [(attr :multival, :multival? true, :limit 100)])
    ['(limit :multival 100)]   (pattern :attrs [(attr :multival, :multival? true, :limit 100)])
    ['(limit :multival nil)]   (pattern :attrs [(attr :multival, :multival? true, :limit nil)])
    ['("limit" :multival 100)] (pattern :attrs [(attr :multival, :multival? true, :limit 100)])
    [['limit :multival 100]]   (pattern :attrs [(attr :multival, :multival? true, :limit 100)])

    ; default
    ['(:multival :default :xyz)]  (pattern :attrs [(attr :multival, :multival? true, :limit 1000, :default :xyz)])
    ['(default :multival :xyz)]   (pattern :attrs [(attr :multival, :multival? true, :limit 1000, :default :xyz)])
    ['("default" :multival :xyz)] (pattern :attrs [(attr :multival, :multival? true, :limit 1000, :default :xyz)])
    [['default :multival :xyz]]   (pattern :attrs [(attr :multival, :multival? true, :limit 1000, :default :xyz)])

    ; xform
    [[:normal :xform 'inc]] (pattern :attrs [(attr :normal :xform inc)])
    [[:normal :xform inc]] (pattern :attrs [(attr :normal :xform inc)])
    #?@(:clj [[[:normal :xform 'datascript.db/datom?]] (pattern :attrs [(attr :normal :xform db/datom?)])])

    ; combined
    ['(:multival :limit 100 :default :xyz :as :other :xform inc)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100, :as :other, :xform inc)])
    ['(:multival :xform inc :as :other :default :xyz :limit 100)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100, :as :other, :xform inc)])
    ['((:multival :limit 100) :default :xyz)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    ['((:multival :default :xyz) :limit 100)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])

    ; combined
    ['(limit (default :multival :xyz) 100)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    ['(default (limit :multival 100) :xyz)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    ['(limit (:multival :default :xyz) 100)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    ['(default (:multival :limit 100) :xyz)] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    ['(((limit :multival 100) :default :xyz))] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    ['(((default :multival :xyz) :limit 100))] (pattern :attrs [(attr :multival, :multival? true, :default :xyz, :limit 100)])
    
    ; repeated
    [:multival [:multival :default :xyz] [:multival :limit 100]] (pattern :attrs [(attr :multival, :multival? true, :limit 100)])
    [:ref {:ref '...}] (pattern :attrs [(attr :ref, :ref? true, :pattern nil, :recursive? true, :recursion-limit nil)])
    [{:ref '...} :ref] (pattern :attrs [(attr :ref, :ref? true)])
    
    ; map spec
    [{:ref [:normal]}]                    (pattern :attrs [(attr :ref, :ref? true, :pattern (pattern :attrs [(attr :normal)]))])
    [{:_ref [:normal]}]                   (pattern :reverse-attrs [(attr :ref, :as :_ref, :ref? true, :reverse? true, :pattern (pattern :attrs [(attr :normal)]))])
    [{:ref '[*]}]                         (pattern :attrs [(attr :ref, :ref? true, :pattern (pattern :wildcard? true, :attrs [(attr :db/id)]))])
    [{:ref [{:ref2 [{:ref3 '[*]}]}]}]     (pattern :attrs [(attr :ref, :ref? true, :pattern (pattern :attrs [(attr :ref2, :ref? true, :pattern (pattern :attrs [(attr :ref3, :ref? true, :pattern (pattern :wildcard? true, :attrs [(attr :db/id)]))]))]))])
    [{:ref [:normal] :ref2 [:normal2]}]   (pattern :attrs [(attr :ref, :ref? true, :pattern (pattern :attrs [(attr :normal)])) (attr :ref2, :ref? true, :pattern (pattern :attrs [(attr :normal2)]))])
    [{:ref [:normal]} {:ref2 [:normal2]}] (pattern :attrs [(attr :ref, :ref? true, :pattern (pattern :attrs [(attr :normal)])) (attr :ref2, :ref? true, :pattern (pattern :attrs [(attr :normal2)]))])
    [{'(:multiref :limit 100) [:normal]}] (pattern :attrs [(attr :multiref, :ref? true, :multival? true, :limit 100, :pattern (pattern :attrs [(attr :normal)]))])
    [{'(limit :multiref 100) [:normal]}]  (pattern :attrs [(attr :multiref, :ref? true, :multival? true, :limit 100, :pattern (pattern :attrs [(attr :normal)]))])
    [{:component 1}]                      (pattern :attrs [(attr :component, :ref? true, :component? true, :pattern nil, :recursive? true, :recursion-limit 1)])

    ; map spec limits
    [{:ref 100}]   (pattern :attrs         [(attr :ref,            :ref? true,                 :pattern nil, :recursive? true, :recursion-limit 100)])
    [{:ref '...}]  (pattern :attrs         [(attr :ref,            :ref? true,                 :pattern nil, :recursive? true, :recursion-limit nil)]) 
    [{:ref "..."}] (pattern :attrs         [(attr :ref,            :ref? true,                 :pattern nil, :recursive? true, :recursion-limit nil)])
    [{:_ref 100}]  (pattern :reverse-attrs [(attr :ref, :as :_ref, :ref? true, :reverse? true, :pattern nil, :recursive? true, :recursion-limit 100)])
    [{:_ref '...}] (pattern :reverse-attrs [(attr :ref, :as :_ref, :ref? true, :reverse? true, :pattern nil, :recursive? true, :recursion-limit nil)]))

  (testing "Error reporting"
    (are [pattern msg] (thrown-msg? msg (dpp/parse-pattern @*db pattern))
      ; refs
      [:_normal] "Expected reverse attribute having :db.type/ref, got: :_normal"

      ; attr-expr
      ['(:multival :limit)] "Expected even number of opts, got: (:multival :limit)"
      
      ; limit
      ['(limit :multival)] "Expected ['limit attr-name (positive-number | nil)], got: (limit :multival)"
      ['(:normal :limit 100)] "Expected limit attribute having :db.cardinality/many, got: :normal"      
      ['(limit :normal 100)]  "Expected limit attribute having :db.cardinality/many, got: :normal"
      ['(:multival :limit :abc)] "Expected (positive-number | nil), got: :abc"
      ['(limit :multival :abc)]  "Expected (positive-number | nil), got: :abc"

      ; default
      ['(default :normal)] "Expected ['default attr-name any-value], got: (default :normal)"
      ['(default :normal 1 2)] "Expected ['default attr-name any-value], got: (default :normal 1 2)"

      ; xform
      [[:normal :xform 'unknown]] "Can't resolve symbol unknown"

      ; map spec
      [{:normal [:normal2]}] "Expected attribute having :db.type/ref, got: :normal"
      [{'(:ref :limit 100) [:normal]}] "Expected limit attribute having :db.cardinality/many, got: :ref"
      [{:ref :normal}] "Expected pattern to be sequential?, got: :normal")))
