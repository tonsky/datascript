(ns datascript.test.lru
  (:require
    [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [is are deftest testing]]
    [datascript.lru :as lru]))

(deftest test-lru
  (let [l0 (lru/lru 2)
        l1 (assoc l0 :a 1)
        l2 (assoc l1 :b 2)
        l3 (assoc l2 :c 3)
        l4 (assoc l3 :b 4)
        l5 (assoc l4 :d 5)]
    (are [l k v] (= (get l k) v)
      l0 :a nil
      l1 :a 1
      l2 :a 1
      l2 :b 2
      l3 :a nil ;; :a get evicted on third insert
      l3 :b 2
      l3 :c 3
      l4 :b 2 ;; assoc updates access time, but does not change a value
      l4 :c 3
      l5 :b 2   ;; :b remains
      l5 :c nil ;; :c gets evicted as the oldest one
      l5 :d 5)))
        
