(ns pi-matrix-relay.commands-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.commands :as commands]))

(deftest parse-command-subcommands
  (testing "setup/status/help and connection controls"
    (is (= {:op :help} (commands/parse "")))
    (is (= {:op :help} (commands/parse "help")))
    (is (= {:op :setup} (commands/parse "setup")))
    (is (= {:op :status} (commands/parse "status")))
    (is (= {:op :control :action "connect"} (commands/parse "connect")))
    (is (= {:op :control :action "disconnect"} (commands/parse "disconnect")))
    (is (= {:op :control :action "reconnect"} (commands/parse "reconnect"))))
  (testing "room bind keeps room, optional alias, and optional mode separate"
    (is (= {:op :room-bind
            :room "#pi:example.org"
            :alias "project"}
           (commands/parse "room bind #pi:example.org project")))
    (is (= {:op :room-bind
            :room "#pi:example.org"
            :alias "project"
            :mode "commands-only"}
           (commands/parse "room bind #pi:example.org project commands-only")))
    (is (= {:op :room-bind
            :room "!abc:example.org"
            :mode "mentions"}
           (commands/parse "room bind !abc:example.org mention"))))
  (testing "room mode parses target and prompt mode"
    (is (= {:op :room-prompt-mode
            :target "ops"
            :mode "all"}
           (commands/parse "room mode ops all"))))
  (testing "send preserves spaces in the message body"
    (is (= {:op :send
            :target "project"
            :message "hello Matrix from pi"}
           (commands/parse "send project hello Matrix from pi"))))
  (testing "progress visibility parses quiet, normal, and verbose verbosity"
    (is (= {:op :progress-verbosity
            :verbosity "quiet"}
           (commands/parse "progress quiet")))
    (is (= {:op :progress-verbosity
            :verbosity "normal"}
           (commands/parse "progress normal")))
    (is (= {:op :progress-verbosity
            :verbosity "verbose"}
           (commands/parse "progress verbose"))))
  (testing "internal new-session bridge command"
    (is (= {:op :internal-new-session
            :request-id "req-123"}
           (commands/parse "__new-session req-123"))))
  (testing "invalid command returns an error data shape"
    (is (= :error (:op (commands/parse "room"))))
    (is (= :error (:op (commands/parse "send only-target"))))
    (is (= :error (:op (commands/parse "progress"))))
    (is (= :error (:op (commands/parse "progress chatty"))))))