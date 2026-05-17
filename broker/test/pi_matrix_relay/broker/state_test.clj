(ns pi-matrix-relay.broker.state-test
  (:require [clojure.test :refer [deftest is testing]]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.test-util :as tu]))

(deftest broker-api-env-does-not-require-public-state-atom
  (testing "Phase 1 removes the legacy state atom from the HTTP API boundary"
    (let [conn (tu/test-db-conn)]
      (try
        (let [env (tu/test-env (tu/fake-gateway) conn)
              app (api/app env)]
          (is (= {:ok true
                  :data {:clientId "state-free-client"
                         :eventStream "/v1/clients/state-free-client/events"
                         :heartbeatSeconds 30
                         :globalOperators ["@operator:example.org"]}}
                 (tu/json-request app :post "/v1/clients"
                                  {:requestId "register-state-free"
                                   :instanceId "state-free-client"
                                   :protocolVersion 1
                                   :project {:id "project"}}))))
        (finally
          (db/release-conn! conn))))))
