(ns datascript.test.parser-return-map
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datascript.core :as d]
    [datascript.parser :as dp]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-parse-return-map
  (is (= (:qreturn-map (dp/parse-query '[:find ?a ?b :keys x y :where [?a ?b]]))
        (dp/->ReturnMap :keys [:x :y])))

  (is (= (:qreturn-map (dp/parse-query '[:find ?a :syms x :where [?a]]))
        (dp/->ReturnMap :syms ['x])))

  (is (= (:qreturn-map (dp/parse-query '[:find ?a ?b ?c :strs x y z :where [?a ?b ?c]]))
        (dp/->ReturnMap :strs ["x" "y" "z"])))


  (testing "with find specs"
    (is (= (:qreturn-map (dp/parse-query '[:find [?a ?b] :keys x y :where [?a ?b]]))
          (dp/->ReturnMap :keys [:x :y])))

    (is (thrown-msg? ":keys does not work with collection :find"
          (dp/parse-query '[:find [?a ...] :keys x :where [?a]])))

    (is (thrown-msg? ":keys does not work with single-scalar :find"
          (dp/parse-query '[:find ?a . :keys x y :where [?a]]))))


  (testing "errors"
    (is (thrown-msg? "Only one of :keys/:syms/:strs must be present"
          (dp/parse-query '[:find ?a ?b :keys x y :strs zt :where [?a ?b]])))

    (is (thrown-msg? "Count of :keys must match count of :find"
          (dp/parse-query '[:find ?a ?b :keys x y z :where [?a ?b]])))

    (is (thrown-msg? "Count of :syms must match count of :find"
          (dp/parse-query '[:find ?a ?b :syms x :where [?a ?b]])))

    (is (thrown-msg? "Count of :strs must match count of :find"
          (dp/parse-query '[:find ?a ?b :strs x :where [?a ?b]])))

    (is (thrown-msg? "Count of :keys must match count of :find"
          (dp/parse-query '[:find [?a ?b] :keys x :where [?a ?b]]))))
)