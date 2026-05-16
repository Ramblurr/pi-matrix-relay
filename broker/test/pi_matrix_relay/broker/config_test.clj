(ns pi-matrix-relay.broker.config-test
  (:require [clojure.test :refer [deftest is testing]]
            [pi-matrix-relay.broker.config :as config]
            [pi-matrix-relay.broker.paths :as paths]))

(deftest config-deep-merge-preserves-defaults-and-ignores-nil-inputs
  (testing "runtime overrides do not erase nested defaults"
    (is (= {:matrix {:encrypted? true
                     :operators ["@alice:example.org"]
                     :device-name "pi-matrix-relay-broker"
                     :homeserver-url "https://matrix.example.org"}
            :http {:transport :tcp :port 0}
            :leases {:heartbeat-seconds 10
                     :suspect-after-missed 2
                     :stale-after-missed 3}
            :events {:buffer-size 512}}
           (config/deep-merge
            config/default-config
            nil
            {:matrix {:homeserver-url "https://matrix.example.org"
                      :operators ["@alice:example.org"]}
             :http {:transport :tcp :port 0}
             :leases {:heartbeat-seconds 10}})))))

(deftest xdg-path-map-has-broker-files-under-app-directory
  (testing "path shape is centralized and data-only"
    (let [p (paths/xdg-paths)]
      (is (= {:config-json? true
              :token? true
              :state-db? true
              :runtime-socket? true}
             {:config-json? (.endsWith (:config-path p) "pi-matrix-relay/config.json")
              :token? (.endsWith (:token-path p) "pi-matrix-relay/token")
              :state-db? (.endsWith (:database-path p) "pi-matrix-relay/trixnity.sqlite")
              :runtime-socket? (.endsWith (:socket-path p) "pi-matrix-relay/broker.sock")})))))
