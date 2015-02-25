(ns datascript.parser)

(defmacro deftrecord
  "Augment all datascript.parser/ records with default implementation of ITraversable"
  [tagname fields & rest]
  (let [f    (gensym "f")
        pred (gensym "pred")
        acc  (gensym "acc")]
   `(defrecord ~tagname ~fields
      ITraversable
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
