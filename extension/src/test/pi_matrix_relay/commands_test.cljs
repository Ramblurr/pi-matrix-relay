(ns pi-matrix-relay.commands-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.commands :as commands]))

(deftest parse-command-subcommands
  (testing "setup/status/help"
    (is (= {:op :help} (commands/parse "")))
    (is (= {:op :setup} (commands/parse "setup")))
    (is (= {:op :status} (commands/parse "status"))))
  (testing "room bind keeps room and optional alias separate"
    (is (= {:op :room-bind
            :room "#pi:example.org"
            :alias "project"}
           (commands/parse "room bind #pi:example.org project")))
    (is (= {:op :room-bind
            :room "!abc:example.org"}
           (commands/parse "room bind !abc:example.org"))))
  (testing "send preserves spaces in the message body"
    (is (= {:op :send
            :target "project"
            :message "hello Matrix from pi"}
           (commands/parse "send project hello Matrix from pi"))))
  (testing "invalid command returns an error data shape"
    (is (= :error (:op (commands/parse "room"))))
    (is (= :error (:op (commands/parse "send only-target"))))))
