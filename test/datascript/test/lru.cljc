(ns datascript.test.lru
  (:require
    [clojure.test :as t :refer [is are deftest testing]]
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

(deftest test-cache
  (let [cache  (lru/cache 2)
        a-time (volatile! 0)
        b-time (volatile! 0)
        c-time (volatile! 0)
        a-fn   #(do (vswap! a-time inc) 1)
        b-fn   #(do (vswap! b-time inc) 2)
        c-fn   #(do (vswap! c-time inc) 3)]
    (is (= 1 (lru/-get cache :a a-fn)))
    (is (= 2 (lru/-get cache :b b-fn)))
    (is (= 1 (lru/-get cache :a a-fn))) ;; :a is now newer
    (is (= 3 (lru/-get cache :c c-fn))) ;; :b is evicted instead
    (is (= 2 (lru/-get cache :b b-fn)))

    (is (= 1 @a-time))
    (is (= 2 @b-time))  ;; b-fn runs twice
    (is (= 1 @c-time))))
