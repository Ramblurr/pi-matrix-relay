(ns pi-matrix-relay.config-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.config :as config]))

(deftest path-helpers-follow-xdg-and-project-layout
  (testing "global config paths prefer XDG_CONFIG_HOME"
    (is (= {:config-dir "/tmp/xdg/pi-matrix-relay"
            :config-path "/tmp/xdg/pi-matrix-relay/config.json"
            :token-path "/tmp/xdg/pi-matrix-relay/token"}
           (config/global-paths #js {"XDG_CONFIG_HOME" "/tmp/xdg"
                                     "HOME" "/home/alice"}))))
  (testing "project config lives below the exact cwd"
    (is (= "/work/project/.agents/matrix-relay/config.json"
           (config/project-config-path "/work/project")))))

(deftest room-binding-defaults-and-target-resolution
  (let [room-result {:roomId "!room:example.org"
                     :canonicalAlias "#pi:example.org"
                     :name "Pi Room"}
        by-default (config/bind-room {} room-result nil "/work/project")
        by-alias (config/bind-room {} room-result "ops" "/work/project")]
    (testing "binding defaults to Matrix alias/name and safe room defaults"
      (is (= {:rooms {"#pi:example.org" {:alias "#pi:example.org"
                                          :roomId "!room:example.org"
                                          :canonicalAlias "#pi:example.org"
                                          :name "Pi Room"
                                          :mode "mentions"
                                          :busy "follow-up"}}}
             by-default)))
    (testing "explicit local aliases are honored"
      (is (= "ops" (get-in by-alias [:rooms "ops" :alias]))))
    (testing "targets resolve by local alias or bound raw room id only"
      (is (= {:roomId "!room:example.org"
              :alias "ops"}
             (select-keys (config/resolve-target by-alias "ops") [:roomId :alias])))
      (is (= {:roomId "!room:example.org"
              :alias "ops"}
             (select-keys (config/resolve-target by-alias "!room:example.org") [:roomId :alias])))
      (is (nil? (config/resolve-target by-alias "!other:example.org"))))))

(deftest target-resolution-tolerates-json-read-keywordized-room-aliases
  (testing "project configs read from JSON can keywordize room-map keys"
    (is (= {:roomId "!room:example.org"
            :alias "ops"}
           (select-keys (config/resolve-target {:rooms {:ops {:alias "ops"
                                                              :roomId "!room:example.org"}}}
                                               "ops")
                        [:roomId :alias])))))
