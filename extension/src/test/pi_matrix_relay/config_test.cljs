(ns pi-matrix-relay.config-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.config :as config]))

(deftest path-helpers-follow-xdg-and-project-layout
  (testing "global config paths prefer XDG_CONFIG_HOME"
    (is (= {:config-dir "/tmp/xdg/pi-matrix-relay"
            :config-path "/tmp/xdg/pi-matrix-relay/config.edn"
            :token-path "/tmp/xdg/pi-matrix-relay/token"}
           (config/global-paths #js {"XDG_CONFIG_HOME" "/tmp/xdg"
                                     "HOME" "/home/alice"}))))
  (testing "project config lives below the exact cwd"
    (is (= "/work/project/.agents/matrix-relay/config.edn"
           (config/project-config-path "/work/project")))))

(deftest room-binding-defaults-and-target-resolution
  (let [room-result {:room/id "!room:example.org"
                     :room/canonical-alias "#pi:example.org"
                     :room/name "Pi Room"}
        by-default (config/bind-room {} room-result nil "/work/project")
        by-alias (config/bind-room {} room-result "ops" "/work/project")]
    (testing "binding defaults to Matrix alias/name and safe room defaults"
      (is (= {:rooms {"#pi:example.org" {:alias "#pi:example.org"
                                          :room/id "!room:example.org"
                                          :room/canonical-alias "#pi:example.org"
                                          :room/name "Pi Room"
                                          :mode "mentions"}}}
             by-default)))
    (testing "explicit local aliases are honored"
      (is (= "ops" (get-in by-alias [:rooms "ops" :alias]))))
    (testing "targets resolve by local alias or bound raw room id only"
      (is (= {:room/id "!room:example.org"
              :alias "ops"}
             (select-keys (config/resolve-target by-alias "ops") [:room/id :alias])))
      (is (= {:room/id "!room:example.org"
              :alias "ops"}
             (select-keys (config/resolve-target by-alias "!room:example.org") [:room/id :alias])))
      (is (nil? (config/resolve-target by-alias "!other:example.org"))))))

(deftest project-config-edn-round-trips-namespaced-room-keys
  (let [cwd (str "/tmp/pi-matrix-relay-config-test-" (random-uuid))
        project-config {:rooms {"ops" {:alias "ops"
                                       :room/id "!room:example.org"
                                       :room/name "Ops Room"
                                       :mode "mentions"}}}]
    (try
      (config/write-project-config! cwd project-config)
      (is (= project-config (config/read-project-config! cwd)))
      (finally
        (.rmSync config/fs cwd #js {:recursive true :force true})))))


(deftest progress-verbosity-defaults-normal-and-accepts-quiet-normal-verbose
  (testing "project config defaults to normal progress visibility"
    (is (= "normal" (config/progress-verbosity {}))))
  (testing "configured verbosity is normalized"
    (is (= "quiet" (config/progress-verbosity {:progress {:verbosity "QUIET"}})))
    (is (= "normal" (config/progress-verbosity {:progress {:verbosity "normal"}})))
    (is (= "verbose" (config/progress-verbosity {:progress {:verbosity "verbose"}}))))
  (testing "invalid verbosity is rejected for command validation"
    (is (config/valid-progress-verbosity? "quiet"))
    (is (config/valid-progress-verbosity? "normal"))
    (is (config/valid-progress-verbosity? "verbose"))
    (is (not (config/valid-progress-verbosity? "chatty")))))