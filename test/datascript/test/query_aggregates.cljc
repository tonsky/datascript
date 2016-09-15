(ns datascript.test.query-aggregates
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))


(defn sort-reverse [xs]
  (reverse (sort xs)))


(deftest test-aggregates
  (let [monsters [ ["Cerberus" 3]
                   ["Medusa" 1]
                   ["Cyclops" 1]
                   ["Chimera" 1] ]]
    (testing "with"
      (is (= (d/q '[ :find ?heads
                     :with ?monster
                     :in   [[?monster ?heads]] ]
                  [ ["Medusa" 1]
                    ["Cyclops" 1]
                    ["Chimera" 1] ])
             [[1] [1] [1]])))

    (testing "Wrong grouping without :with"
      (is (= (d/q '[ :find (sum ?heads)
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[4]])))

    (testing "Multiple aggregates, correct grouping with :with"
      (is (= (d/q '[ :find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
                     :with ?monster
                     :in   [[?monster ?heads]] ]
                  monsters)
             [[6 1 3 4 2]])))
    
    (testing "Min and max are using comparator instead of default compare"
      ;; Wrong: using js '<' operator
      ;; (apply min [:a/b :a-/b :a/c]) => :a-/b
      ;; (apply max [:a/b :a-/b :a/c]) => :a/c
      ;; Correct: use IComparable interface
      ;; (sort compare [:a/b :a-/b :a/c]) => (:a/b :a/c :a-/b)
      (is (= (d/q '[:find (min ?x) (max ?x)
                    :in [?x ...]]
                  [:a-/b :a/b])
             [[:a/b :a-/b]]))

      (is (= (d/q '[:find (min 2 ?x) (max 2 ?x)
                    :in [?x ...]]
                  [:a/b :a-/b :a/c])
             [[[:a/b :a/c] [:a/c :a-/b]]])))

    (testing "Grouping and parameter passing"
      (is (= (set (d/q '[ :find ?color (max ?amount ?x) (min ?amount ?x)
                          :in   [[?color ?x]] ?amount ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       3))
             #{[:red  [3 4 5] [1 2 3]]
               [:blue [7 8]   [7 8]]})))

    (testing "avg aggregate" 
      (is (= (ffirst (d/q '[:find (avg ?x) 
                            :in [?x ...]]
                           [10 15 20 35 75]))
             31)))

    (testing "median aggregate"
      (is (= (ffirst (d/q '[:find (median ?x)
                            :in [?x ...]]
                           [10 15 20 35 75]))
             20)))
    
    (testing "variance aggregate"
      (is (= (ffirst (d/q '[:find (variance ?x)
                            :in [?x ...]]
                           [10 15 20 35 75]))
              554)))

    (testing "stddev aggregate"
      (is (= (ffirst (d/q '[:find (stddev ?x) 
                            :in [?x ...]]
                          [10 15 20 35 75]))
              23.53720459187964)))

    (testing "Custom aggregates"
      (let [data   [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                    [:blue 7] [:blue 8]]
            result #{[:red [5 4 3 2 1]] [:blue [8 7]]}]
        
        (is (= (set (d/q '[ :find ?color (aggregate ?agg ?x)
                            :in   [[?color ?x]] ?agg ]
                         data
                         sort-reverse))
               result))
        
        #?(:clj
            (is (= (set (d/q '[ :find ?color (datascript.test.query-aggregates/sort-reverse ?x)
                                :in   [[?color ?x]]]
                             data))
                   result)))
        ))))
