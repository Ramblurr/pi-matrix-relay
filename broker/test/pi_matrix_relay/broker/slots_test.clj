(ns pi-matrix-relay.broker.slots-test
  (:require [clojure.test :refer [deftest is testing]]
            [pi-matrix-relay.broker.slots :as slots]))

(deftest slot-labels-are-excel-style
  (testing "slot labels grow from A through multi-letter labels"
    (is (= ["A" "B" "Z" "AA" "AB" "AZ" "BA" "ZZ" "AAA"]
           (mapv slots/slot-label [0 1 25 26 27 51 52 701 702])))))
