(ns datascript.pull-parser
  (:require
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [raise]]))

(defrecord PullSpec [wildcard? attrs])

(defprotocol IPullSpecComponent
  (-as-spec [this]))

(defrecord PullAttrName [attr]
  IPullSpecComponent
  (-as-spec [this]
    [attr {:attr attr}]))

(defrecord PullReverseAttrName [attr rattr]
  IPullSpecComponent
  (-as-spec [this]
    [rattr {:attr attr}]))

(defrecord PullLimitExpr [attr limit]
  IPullSpecComponent
  (-as-spec [this]
    (-> (-as-spec attr)
        (assoc-in [1 :limit] limit))))

(defrecord PullDefaultExpr [attr value]
  IPullSpecComponent
  (-as-spec [this]
    (-> (-as-spec attr)
        (assoc-in [1 :default] value))))

(defrecord PullWildcard [])

(defrecord PullRecursionLimit [limit]
  IPullSpecComponent
  (-as-spec [this]
    [:recursion limit]))

(defrecord PullMapSpecEntry [attr porrl]
  IPullSpecComponent
  (-as-spec [this]
    (-> (-as-spec attr)
        (update 1 conj (-as-spec porrl)))))

(defn- aggregate-specs
  [res part]
  (if (instance? PullWildcard part)
    (assoc res :wildcard? true)
    (update res :attrs conj! (-as-spec part))))

(defrecord PullPattern [specs]
  IPullSpecComponent
  (-as-spec [this]
    (let [init (PullSpec. false (transient {}))
          spec (reduce aggregate-specs init specs)]
      [:subpattern (update spec :attrs persistent!)])))

(declare parse-pattern)

(def ^:private wildcard? #{'* :* "*"})

(defn- parse-wildcard
  [spec]
  (when (wildcard? spec)
    (PullWildcard.)))

(defn- parse-attr-name
  [spec]
  (when (or (keyword? spec) (string? spec))
    (if (db/reverse-ref? spec)
      (PullReverseAttrName. (db/reverse-ref spec) spec)
      (PullAttrName. spec))))

(def ^:private unlimited-recursion? #{'... "..."})

(defn- parse-recursion-limit
  [spec]
  (cond
    (unlimited-recursion? spec)
    (PullRecursionLimit. nil)

    (and (number? spec) (pos? spec))
    (PullRecursionLimit. spec)))

(defn- maybe-attr-expr?
  [spec]
  (and (sequential? spec) (= 3 (count spec))))

(def ^:private limit? #{'limit :limit "limit"})

(defn- parse-limit-expr
  [spec]
  (let [[limit-sym attr-name-spec pos-num] spec]
    (when (limit? limit-sym)
      (if-let [attr-name (and (or (nil? pos-num)
                                  (and (number? pos-num) (pos? pos-num)))
                              (parse-attr-name attr-name-spec))]
        (PullLimitExpr. attr-name pos-num)
        (raise "Expected [\"limit\" attr-name (positive-number | nil)]"
               {:error :parser/pull, :fragment spec})))))

(def ^:private default? #{'default :default "default"})

(defn- parse-default-expr
  [spec]
  (let [[default-sym attr-name-spec default-val] spec]
    (when (default? default-sym)
      (if-let [attr-name (and (default? default-sym)
                              (parse-attr-name attr-name-spec))]
        (PullDefaultExpr. attr-name default-val)
        (raise "Expected [\"default\" attr-name any-value]"
               {:error :parser/pull, :fragment spec})))))

(defn- parse-map-spec-entry
  [[k v]]
  (if-let [attr-name (or (parse-attr-name k)
                         (when (maybe-attr-expr? k)
                           (parse-limit-expr k)))]
    (if-let [pattern-or-rec (or (parse-recursion-limit v)
                                (parse-pattern v))]
      (PullMapSpecEntry. attr-name pattern-or-rec)
      (raise "Expected (pattern | recursion-limit)"
             {:error :parser/pull, :fragment [k v]}))
    (raise "Expected (attr-name | limit-expr)"
           {:error :parser/pull, :fragment [k v]})))

(defn- parse-map-spec
  [spec]
  (when (map? spec)
    (assert (= 1 (count spec)) "Maps should contain exactly 1 entry")
    (parse-map-spec-entry (first spec))))

(defn- parse-attr-expr
  [spec]
  (when (maybe-attr-expr? spec)
    (or (parse-limit-expr spec)
        (parse-default-expr spec))))

(defn- parse-attr-spec
  [spec]
  (or (parse-attr-name spec)
      (parse-wildcard spec)
      (parse-map-spec spec)
      (parse-attr-expr spec)
      (raise "Cannot parse attr-spec, expected: (attr-name | wildcard | map-spec | attr-expr)"
             {:error :parser/pull, :fragment spec})))

(defn- pattern-clause-type
  [clause]
  (cond
    (map? clause)      :map
    (wildcard? clause) :wildcard
    :else              :other))

(defn- expand-map-clause
  [clause]
  (into [] (map #(conj {} %)) clause))

(defn- simplify-pattern-clauses
  [pattern]
  (let [groups (group-by pattern-clause-type pattern)
        base   (if (not-empty (get groups :wildcard))
                 ['*] [])]
    (-> base
        (into (get groups :other))
        (into (mapcat expand-map-clause) (get groups :map)))))

(defn parse-pattern
  "Parse an EDN pull pattern into a tree of records using the following
grammar:

```
pattern            = [attr-spec+]
attr-spec          = attr-name | wildcard | map-spec | attr-expr
attr-name          = an edn keyword that names an attr
wildcard           = \"*\" or '*'
map-spec           = { ((attr-name | limit-expr) (pattern | recursion-limit))+ }
attr-expr          = limit-expr | default-expr
limit-expr         = [\"limit\" attr-name (positive-number | nil)]
default-expr       = [\"default\" attr-name any-value]
recursion-limit    = positive-number | '...'
```"
  [pattern]
  (when (sequential? pattern)
    (->> pattern
         simplify-pattern-clauses
         (into [] (map parse-attr-spec))
         (PullPattern.))))

(defn pattern->spec
  "Convert a parsed tree of pull pattern records into a `PullSpec` instance,
a record type containing two keys:

* `:wildcard?` - a boolean indicating if the pattern contains a wildcard.
* `:attrs` - a map of attribute specifications.

The attribute specification map consists of keys which will become the keys
in the result map, and values which are themselves maps describing the
attribute:

* `:attr`       (required) - The attr name to pull; for reverse attributes
                             this will be the normalized attribute name.
* `:limit`      (optional) - If present, specifies a custom limit for this
                             attribute; Either `nil`, indicating no limit,
                             or a positive integer.
* `:default`    (optional) - If present, specifies a default value for this
                             attribute
* `:recursion`  (optional) - If present, specifies a recursion limit for this
                             attribute; Either `nil`, indicating no limit, or
                             a positive integer.
* `:subpattern` (optional) - If present, specifies a sub `PullSpec` instance
                             to be applied to entities matched by this
                             attribute."
  [pattern]
  (nth (-as-spec pattern) 1))

(defn parse-pull
  "Parse EDN pull `pattern` specification (see `parse-pattern`), and
convert the resulting tree into a `PullSpec` instance (see `pattern->spec`).
Throws an error if the supplied `pattern` cannot be parsed."
  [pattern]
  (or (some-> pattern parse-pattern pattern->spec)
      (raise "Cannot parse pull pattern, expected: [attr-spec+]"
             {:error :parser/pull, :fragment pattern})))
