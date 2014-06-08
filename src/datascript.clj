(ns datascript)

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
