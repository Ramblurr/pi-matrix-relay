(ns pi-matrix-relay.systemd-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.systemd :as systemd]))

(def fs (js/require "fs"))
(def path (js/require "path"))
(def os (js/require "os"))

(deftest service-path-follows-user-systemd-config-location
  (is (= "/tmp/xdg/systemd/user/pi-matrix-broker.service"
         (systemd/service-path #js {"XDG_CONFIG_HOME" "/tmp/xdg"
                                    "HOME" "/home/alice"}))))

(deftest render-service-starts-the-jvm-broker-from-broker-directory
  (let [service (systemd/render-service {:broker-dir "/repo/pi-matrix-relay/broker"
                                          :bb-command "/nix/store/devshell/bin/bb"
                                          :path-env "/nix/store/devshell/bin:/run/current-system/sw/bin"})]
    (is (re-find #"Description=Pi Matrix Relay broker" service))
    (is (re-find #"Environment=PATH=/nix/store/devshell/bin:/run/current-system/sw/bin" service))
    (is (re-find #"WorkingDirectory=/repo/pi-matrix-relay/broker" service))
    (is (re-find #"ExecStart=/nix/store/devshell/bin/bb broker" service))
    (is (re-find #"WantedBy=default.target" service))))

(deftest service-options-capture-current-path-for-systemd
  (let [tmp-dir (.mkdtempSync fs (.join path (.tmpdir os) "pi-matrix-relay-systemd-test-"))
        bb-path (.join path tmp-dir "bb")]
    (.writeFileSync fs bb-path "#!/bin/sh\n" "utf8")
    (let [opts (systemd/service-opts #js {"PATH" (str tmp-dir ":/usr/bin")})]
      (is (= (str tmp-dir ":/usr/bin") (:path-env opts)))
      (is (= bb-path (:bb-command opts))))))
