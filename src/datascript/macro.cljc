(ns datascript.macro)

;; isolate all macros to a single location, because CLJS compilation
;; cannot handle inline macros in CLJC files.
;; XXX even weirder, it can't be in a .clj file, but appears to need
;; to be in .cljc. So wrap it all in :clj guard.
;; Caused by: clojure.lang.ExceptionInfo: No such namespace: datascript.macro, could not locate datascript/macro.cljs or datascript/macro.cljc at line 1 src/datascript/core.cljc

#?(:clj (do

(defmacro raise [& fragments]
  (let [msgs (butlast fragments)
        data (last fragments)]
   `(throw (ex-info (str ~@(map #(if (string? %) % (list 'pr-str %)) msgs)) ~data))))

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

(defmacro deftrecord
  "Augment all datascript.parser/records with default implementation of ITraversable"
  [tagname fields & rest]
  (let [f     (gensym "f")
        pred  (gensym "pred")
        acc   (gensym "acc")]
    `(defrecord ~tagname ~fields
       datascript.parser/ITraversable
       (~'-postwalk [this# ~f]
         (let [new# (new ~tagname ~@(map #(list 'datascript.parser/postwalk % f) fields))]
           (set! (.-__meta new#) (meta this#))
           new#))
       (~'-collect [_# ~pred ~acc]
         ;; [x y z] -> (collect pred z (collect pred y (collect pred x acc)))
         ~(reduce #(list 'datascript.parser/collect pred %2 %1) acc fields))
       (~'-collect-vars [_# ~acc]
         ;; [x y z] -> (collect-vars-acc (collect-vars-acc (collect-vars-acc acc x) y) z)
         ~(reduce #(list 'datascript.parser/collect-vars-acc %1 %2) acc fields))
       ~@rest)))

))
