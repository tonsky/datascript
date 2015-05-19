(ns datascript.test.query-aggregates
  (:require
   [#?(:cljs cemerick.cljs.test :clj clojure.test) :as t #?(:cljs :refer-macros :clj :refer) [is are deftest testing]]
   [datascript :as d]
   [datascript.core :as dc]
   [datascript.test.core :as tdc]))

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

    (testing "Min and max comparator order types reliably"
      ;; XXX a bit hackish, but using int-array gives a type name
      ;; of [..., which sorts out before java.*
      (let [date #?(:cljs (js/Date.) :clj (int-array 0))]
        ;; Wrong: js '<' operator coerce everything to string
        ;; (apply min ["1" date 1]) => 1
        ;; (apply max ["1" date 1]) => 1
        ;; Correct: dc/cmp-val compares types first
        ;; (sort dc/cmp-val ["1" date 1]) => (date 1 "1")
        (is (= (d/q '[:find (min ?x) (max ?x)
                      :in [?x ...]]
                    ["1" date 1])
               [[date "1"]]))

        (is (= (d/q '[:find (min 2 ?x) (max 2 ?x)
                      :in [?x ...]]
                    ["1" date 1])
               [[[date 1] [1 "1"]]]))))

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
      (is (= (set (d/q '[ :find ?color (aggregate ?agg ?x)
                          :in   [[?color ?x]] ?agg ]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       #(reverse (sort %))))
             #{[:red [5 4 3 2 1]] [:blue [8 7]]})))))
