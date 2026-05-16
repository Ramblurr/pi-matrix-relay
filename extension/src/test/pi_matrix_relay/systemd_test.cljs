(ns pi-matrix-relay.systemd-test
  (:require [cljs.test :refer [deftest is testing]]
            [pi-matrix-relay.systemd :as systemd]))

(deftest service-path-follows-user-systemd-config-location
  (is (= "/tmp/xdg/systemd/user/pi-matrix-broker.service"
         (systemd/service-path #js {"XDG_CONFIG_HOME" "/tmp/xdg"
                                    "HOME" "/home/alice"}))))

(deftest render-service-starts-the-jvm-broker-from-broker-directory
  (let [service (systemd/render-service {:broker-dir "/repo/pi-matrix-relay/broker"})]
    (is (re-find #"Description=Pi Matrix Relay broker" service))
    (is (re-find #"WorkingDirectory=/repo/pi-matrix-relay/broker" service))
    (is (re-find #"ExecStart=/usr/bin/env bb broker" service))
    (is (re-find #"WantedBy=default.target" service))))
