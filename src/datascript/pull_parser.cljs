(ns datascript.pull-parser
  (:require
   [datascript.core :as dc]))

;; pattern            = [attr-spec+]
;; attr-spec          = attr-name | wildcard | map-spec | attr-expr
;; attr-name          = an edn keyword that names an attr
;; wildcard           = "*" or '*'
;; map-spec           = { ((attr-name | limit-expr) (pattern | recursion-limit))+ }
;; attr-expr          = limit-expr | default-expr
;; limit-expr         = ["limit" attr-name (positive-number | nil)]
;; default-expr       = ["default" attr-name any-value]
;; recursion-limit    = positive-number | '...'

(defprotocol IPullAttrExpr
  (-attr [this]))

(defrecord PullAttrName [attr]
  IPullAttrExpr
  (-attr [this] attr))

(defrecord PullReverseAttrName [attr rattr]
  IPullAttrExpr
  (-attr [this] rattr))

(defrecord PullMapSpecEntry [attr porrl]
  IPullAttrExpr
  (-attr [this] (-attr (.-attr this))))

(defrecord PullLimitExpr [attr limit]
  IPullAttrExpr
  (-attr [this] (-attr (.-attr this))))

(defrecord PullDefaultExpr [attr value]
  IPullAttrExpr
  (-attr [this] (-attr (.-attr this))))

(defrecord PullWildcard [except])
(defrecord PullRecursionLimit [limit])
(defrecord PullPattern [specs])

(declare parse-pattern)

(def ^:private wildcard? #{'* :* "*"})

(defn- parse-wildcard
  [spec]
  (when (wildcard? spec)
    (PullWildcard. #{})))

(defn- parse-attr-name
  [spec]
  (when (or (keyword? spec) (string? spec))
    (if (dc/reverse-ref? spec)
      (PullReverseAttrName. (dc/reverse-ref spec) spec)
      (PullAttrName. spec))))

(defn- parse-recursion-limit
  [spec]
  (cond
    (= '... spec)
    (PullRecursionLimit. nil)

    (and (number? spec) (pos? spec))
    (PullRecursionLimit. spec)))

(defn- maybe-attr-expr?
  [spec]
  (and (sequential? spec) (= 3 (count spec))))

(def ^:private limit? #{'limit "limit"})

(defn- parse-limit-expr
  [spec]
  (let [[limit-sym attr-name-spec pos-num] spec]
    (when (limit? limit-sym)
      (if-let [attr-name (and (or (nil? pos-num)
                                  (and (number? pos-num) (pos? pos-num)))
                              (parse-attr-name attr-name-spec))]
        (PullLimitExpr. attr-name pos-num)
        (throw (js/Error. "Expected [\"limit\" attr-name (positive-number | nil)]"))))))

(def ^:private default? #{'default "default"})

(defn- parse-default-expr
  [spec]
  (let [[default-sym attr-name-spec default-val] spec]
    (when (default? default-sym)
      (if-let [attr-name (and (default? default-sym)
                              (parse-attr-name attr-name-spec))]
        (PullDefaultExpr. attr-name default-val)
        (throw (js/Error. "Expected [\"default\" attr-name any-value]"))))))

(defn- parse-map-spec-entry
  [[k v]]
  (if-let [attr-name (or (parse-attr-name k)
                         (when (maybe-attr-expr? k)
                           (parse-limit-expr k)))]
    (if-let [pattern-or-rec (or (parse-recursion-limit v)
                                (parse-pattern v))]
      (PullMapSpecEntry. attr-name pattern-or-rec)
      (throw (js/Error. "Expected (pattern | recursion-limit)")))
    (throw (js/Error. "Expected (attr-name | limit-expr)"))))

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
      (throw (js/Error. "Cannot parse attr-spec, expected: (attr-name | wildcard | map-spec | attr-expr)"))))

(defn- pattern-clause-type
  [clause]
  (cond
    (map? clause)         :map
    (#{'* :* "*"} clause) :wildcard
    :else                 :other))

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

(defn- parse-pattern
  [pattern]
  (when (sequential? pattern)
    (let [simplified      (simplify-pattern-clauses pattern)
          has-wildcard?   (wildcard? (first simplified))
          parsed          (into [] (map parse-attr-spec) simplified)
          analyzed        (if has-wildcard?
                            (update-in parsed [0 :except] into
                                       (map -attr)
                                       (rest parsed))
                            parsed)]
      (PullPattern. analyzed))))

(defn parse-pull
  [pattern]
  (or (parse-pattern pattern)
      (throw (js/Error. "Cannot parse pull pattern, expected: [attr-spec+]"))))
