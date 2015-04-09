(ns datascript.macros)

;; doesn't exist in 1.6, not worrying about 1.7 yet
(defn update
  ([m k f] (assoc m k (f (get m k))))
  ([m k f x1] (assoc m k (f (get m k) x1)))
  ([m k f x1 x2] (assoc m k (f (get m k) x1 x2)))
  ([m k f x1 x2 & xs] (assoc m k (apply f (get m k) x1 x2 xs))))

(defmacro combine-cmp [& comps]
  (loop [comps (reverse comps)
         res   0]
    (if (not-empty comps)
      (recur
        (next comps)
        `(let [c# ~(first comps)]
           (if (== 0 c#)
             ~res
             c#)))
      res)))

(defn- -case-tree [queries variants]
  (if queries
    (let [v1 (take (/ (count variants) 2) variants)
          v2 (drop (/ (count variants) 2) variants)]
      (list 'if (first queries)
        (-case-tree (next queries) v1)
        (-case-tree (next queries) v2)))
    (first variants)))

(defmacro case-tree [qs vs]
  (-case-tree qs vs))

(defmacro raise [& fragments]
  (let [msgs (butlast fragments)
        data (last fragments)]
   `(throw (ex-info (str ~@(map #(if (string? %) % (list 'pr-str %)) msgs)) ~data))))
