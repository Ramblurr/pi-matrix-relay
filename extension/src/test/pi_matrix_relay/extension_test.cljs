(ns pi-matrix-relay.extension-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.extension :as extension]))

(deftest greeting-includes-target
  (testing "the extension test runner can load project namespaces"
    (is (= "Hello, Matrix, from ClojureScript!"
           (extension/greeting "Matrix")))))
