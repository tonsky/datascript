(ns ^:no-doc datascript.pull-parser
  (:require
    [datascript.built-ins :as built-ins]
    [datascript.db :as db]
    [datascript.util :as util]))

(defrecord PullAttr [as default limit name pattern recursion-limit recursive? reverse? xform multival? ref? component?])

(defrecord PullPattern [attrs first-attr last-attr reverse-attrs wildcard?])

(def default-db-id-attr
  (map->PullAttr {:name :db/id :as :db/id :xform identity}))

(def default-pattern-ref
  (map->PullPattern {:attrs (list default-db-id-attr)}))

(def default-pattern-component
  (assoc default-pattern-ref :wildcard? true))

(declare parse-pattern parse-attr-spec)

; pattern             = [(attr-spec | map-spec | '* | "*")+]
; attr-spec           = attr-name | attr-expr | legacy-limit-expr | legacy-default-expr
; attr-name           = an edn keyword that names an attr
; attr-expr           = [attr-name attr-option+]
; map-spec            = {attr-spec (pattern | recursion-limit)}
; attr-option         = :as any-value | :limit multival-limit | :default any-value | :xform symbol
; recursion-limit     = positive-number | '...
; multival-limit      = positive-number | nil
; legacy-limit-expr   = [("limit" | 'limit) attr-spec multival-limit]
; legacy-default-expr = [("default" | 'default) attr-spec any-value]

(defn check [cond expected fragment]
  (when-not cond
    (throw (ex-info (str "Expected " expected ", got: " (pr-str fragment))
             {:error :parser/pull, :fragment fragment}))))

(defn parse-attr-name [db attr-spec]
  (let [reverse?   (db/reverse-ref? attr-spec)
        name       (if reverse? (db/reverse-ref attr-spec) attr-spec)
        ref?       (db/ref? db name)
        component? (db/component? db name)
        multival?  (db/multival? db name)]
    (map->PullAttr
      {:as         attr-spec
       :name       name
       :xform      identity
       :multival?  (when multival? true)
       :limit      (if multival? 1000 nil)
       :ref?       (when ref? true)
       :component? (when component? true)
       :pattern    (cond
                     (not ref?) nil
                     reverse?   default-pattern-ref
                     component? default-pattern-component
                     :else      default-pattern-ref)
       :reverse?   (when reverse?
                     (check ref? "reverse attribute having :db.type/ref" attr-spec)
                     true)})))

(defn- check-limit [db pull-attr limit]
  (check (or (and (number? limit) (pos? limit)) (nil? limit)) "(positive-number | nil)" limit)
  (check (db/multival? db (:name pull-attr)) "limit attribute having :db.cardinality/many" (:name pull-attr)))

(defn- resolve-xform [sym-or-fn]
  (or
    (when (fn? sym-or-fn)
      sym-or-fn)
    (get built-ins/query-fns sym-or-fn)
    #?(:clj (when (namespace sym-or-fn)
              (when-some [v (requiring-resolve sym-or-fn)]
                @v)))
    (util/raise "Can't resolve symbol " sym-or-fn {:error :parser/pull, :fragment sym-or-fn})))

(defn parse-attr-expr [db attr-spec]
  (when-some [pull-attr (parse-attr-spec db (first attr-spec))]
    (check (even? (count (next attr-spec))) "even number of opts" attr-spec)
    (reduce
      (fn [pull-attr [key value]]
        (case key
          :as      (assoc pull-attr :as value)
          :limit   (do
                     (check-limit db pull-attr value)
                     (assoc pull-attr :limit value))
          :default (assoc pull-attr :default value)
          :xform   (assoc pull-attr :xform (resolve-xform value))
          #_else   (check false "one of :as, :limit, :default, :xform" attr-spec)))
      pull-attr
      (partition 2 (next attr-spec)))))

(defn parse-legacy-limit-expr [db attr-spec]
  (let [expected "['limit attr-name (positive-number | nil)]"]
    (when (#{'limit "limit"} (first attr-spec))
      (check (= (count attr-spec) 3) expected attr-spec)
      (let [[_ attr limit] attr-spec
            pull-attr (parse-attr-spec db attr)]
        (check-limit db pull-attr limit)
        (assoc pull-attr :limit limit)))))

(defn parse-legacy-default-expr [db attr-spec]
  (let [expected "['default attr-name any-value]"]
    (when (#{'default "default"} (first attr-spec))
      (check (= (count attr-spec) 3) expected attr-spec)
      (let [[_ attr default] attr-spec
            pull-attr (parse-attr-spec db attr)]
        (assoc pull-attr :default default)))))

(defn parse-attr-spec [db attr-spec]
  (cond
    (or (keyword? attr-spec)
      (and (string? attr-spec)
        (not (#{"default" "limit"} attr-spec))))
    (parse-attr-name db attr-spec)

    (sequential? attr-spec)
    (or
      (parse-attr-expr db attr-spec)
      (parse-legacy-limit-expr db attr-spec)
      (parse-legacy-default-expr db attr-spec)
      (check false "[attr-name attr-option+] | ['limit attr-name (positive-num | nil)] | ['default attr-name any-val]" attr-spec))

    :else nil))

(defn parse-map-spec [db attr-spec pattern]
  (let [pull-attr (parse-attr-spec db attr-spec)]
    (check (some? pull-attr) "attr-name | attr-expr" attr-spec)
    (check (db/ref? db (:name pull-attr)) "attribute having :db.type/ref" attr-spec)
    (cond
      (or (= '... pattern) (= "..." pattern))
      (assoc pull-attr :pattern nil :recursive? true :recursion-limit nil)

      (number? pattern)
      (do
        (check (pos? pattern) "(positive-num | ...)" {attr-spec pattern})
        (assoc pull-attr :pattern nil :recursive? true :recursion-limit pattern))

      :else
      (assoc pull-attr :pattern (parse-pattern db pattern)))))

(defn- index-of [pred xs]
  (some (fn [[x idx]] (when (pred x) idx)) (map vector xs (range))))

(defn- conj-attr [pull-pattern pull-attr]
  (let [pattern-attr (if (:reverse? pull-attr) :reverse-attrs :attrs)]
    (if-some [idx (index-of #(= (:as %) (:as pull-attr)) (get pull-pattern pattern-attr))]
      (update pull-pattern pattern-attr assoc idx pull-attr)
      (update pull-pattern pattern-attr conj pull-attr))))

(defn parse-pattern ^PullPattern [db pattern]
  (check (sequential? pattern) "pattern to be sequential?" pattern)
  (loop [pattern pattern
         ^PullPattern result (map->PullPattern {:attrs [] :reverse-attrs [] :wildcard? nil})]
    (util/cond+
      (empty? pattern)
      (let [attrs       (.-attrs result)
            db-id?      (fn [^PullAttr attr] (#{:db/id ":db/id"} (.-name attr)))
            key-fn      (fn [^PullAttr attr]
                          (let [name (:name attr)]
                            (cond
                              (keyword? name) name
                              (= ":" (subs name 0 1)) (keyword (subs name 1))
                              :eles (keyword name))))
            attrs       (if (and
                              (.-wildcard? result)
                              (not (some db-id? (.-attrs result))))
                          (conj attrs default-db-id-attr)
                          attrs)
            attrs       (list* (sort-by key-fn attrs))
            datom-attrs (remove db-id? attrs)
            first-attr  (first datom-attrs)
            last-attr   (last datom-attrs)]
        (map->PullPattern
          {:attrs         attrs
           :first-attr    first-attr
           :last-attr     last-attr
           :reverse-attrs (list* (sort-by key-fn (.-reverse-attrs result)))
           :wildcard?     (.-wildcard? result)}))

      :let [attr-spec (first pattern)]

      (or (= '* attr-spec) (= "*" attr-spec) (= :* attr-spec))
      (recur (next pattern) (assoc result :wildcard? true))

      (map? attr-spec)
      (let [result' (reduce-kv
                      (fn [result attr-spec pattern]
                        (conj-attr result (parse-map-spec db attr-spec pattern)))
                      result
                      attr-spec)]
        (recur (next pattern) result'))
        
      :let [pull-attr (parse-attr-spec db attr-spec)]

      (nil? pull-attr)
      (check false "attr-name | attr-expr | map-spec | *" attr-spec)
      
      :else
      (recur (next pattern) (conj-attr result pull-attr)))))
