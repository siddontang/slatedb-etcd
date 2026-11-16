(ns slatedb.check-test
  (:require [clojure.test :refer :all]
            [slatedb.check :as check]))

(defn operation [process function type value]
  {:process process :f function :type type :value value})

(defn pair [process function input output]
  [(operation process function :invoke input) (operation process function :ok output)])

(deftest register-controls
  (let [write (pair 0 :write 1 1)
        read-one (pair 1 :read nil 1)
        read-old (pair 1 :read nil nil)
        valid? #(-> (check/check-history "register" %) :valid?)]
    (is (true? (valid? (concat write read-one))))
    (is (false? (valid? (concat write read-old))))
    (is (false? (valid? (concat write (pair 1 :write 2 2) read-one))))
    (is (true? (valid? (concat [(operation 0 :write :invoke 1)
                               (operation 0 :write :info 1)] read-one))))
    (is (true? (valid? (concat [(operation 0 :write :invoke 1)
                               (operation 0 :write :info 1)] read-old))))
    (is (false? (valid? [])))))

(deftest transaction-controls
  (let [write (pair 0 :txn [[:append :a 1] [:append :b 1]]
                         [[:append :a 1] [:append :b 1]])
        read-both (pair 1 :txn [[:r :a nil] [:r :b nil]] [[:r :a [1]] [:r :b [1]]])
        read-half (pair 1 :txn [[:r :a nil] [:r :b nil]] [[:r :a [1]] [:r :b []]])
        valid? #(-> (check/check-history "append" %) :valid?)]
    (is (true? (valid? (concat write read-both))))
    (is (false? (valid? (concat write read-half))))
    (is (false? (valid? (concat write (pair 1 :txn [[:r :a nil]] [[:r :a []]]))))))
  (is (false? (:valid? (check/check-history "append" [])))))
