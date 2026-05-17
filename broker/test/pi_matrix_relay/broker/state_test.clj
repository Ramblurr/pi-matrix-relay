(ns pi-matrix-relay.broker.state-test
  (:require [clojure.test :refer [deftest is testing]]
            [pi-matrix-relay.broker.state :as state]))

(deftest clients-slots-and-room-authorization-work-together
  (testing "client registration, slot acquisition, authorization, and release mutate broker state coherently"
    (let [state* (atom (state/empty-state))]
      (is (= {:clientId "client-1"
              :eventStream "/v1/clients/client-1/events"
              :heartbeatSeconds 30
              :globalOperators ["@operator:example.org"]}
             (state/register-client!
              state*
              {:client-id-fn (constantly "client-1")
               :now 1000
               :heartbeat-seconds 30
               :global-operators ["@operator:example.org"]}
              {:clientInstanceId "instance-1"
               :protocolVersion 1
               :project {:id "project"}
               :subscriptions {:rooms ["!project:example.org"]}})))
      (let [lease (state/acquire-slot!
                   state*
                   {:now 1100}
                   {:client-id "client-1"
                    :project {:id "project"}
                    :room-id "!slot:example.org"
                    :room-name "project-A"})]
        (is (= {:lease {:slot "A"
                        :room-id "!slot:example.org"
                        :state :leased}
                :known-project-room? true
                :known-slot-room? true
                :unknown-room? false
                :slots {:projectId "project"
                        :slots [{:slot "A"
                                 :roomId "!slot:example.org"
                                 :roomName "project-A"
                                 :clientId "client-1"
                                 :clientMetadata nil
                                 :state "leased"
                                 :acquiredAt 1100
                                 :lastHeartbeatAt 1100}]}}
               {:lease (select-keys lease [:slot :room-id :state])
                :known-project-room? (state/known-room-for-client? @state* "client-1" "!project:example.org")
                :known-slot-room? (state/known-room-for-client? @state* "client-1" "!slot:example.org")
                :unknown-room? (state/known-room-for-client? @state* "client-1" "!other:example.org")
                :slots (state/list-slots @state* "project")}))
        (is (= {:released true
                :known-slot-room? false}
               {:released (:released (state/release-slot! state* {:client-id "client-1" :slot "A"}))
                :known-slot-room? (state/known-room-for-client? @state* "client-1" "!slot:example.org")}))))))

(deftest stale-slot-release-removes-room-authorization
  (testing "stale lease cleanup releases the slot and removes client send authorization"
    (let [state* (atom (state/empty-state))]
      (state/register-client!
       state*
       {:client-id-fn (constantly "client-stale")
        :now 1000
        :heartbeat-seconds 30
        :global-operators []}
       {:clientInstanceId "instance-stale"
        :protocolVersion 1
        :project {:id "project"}})
      (state/acquire-slot!
       state*
       {:now 1000}
       {:client-id "client-stale"
        :project {:id "project"}
        :room-id "!slot-stale:example.org"
        :room-name "project-A"})
      (let [stale (state/mark-stale-leases! state* 100000 30 3)]
        (is (= {:stale-slots ["A"]
                :known-slot-room? false
                :slot-state "released"}
               {:stale-slots (mapv :slot stale)
                :known-slot-room? (state/known-room-for-client? @state* "client-stale" "!slot-stale:example.org")
                :slot-state (get-in (state/list-slots @state* "project") [:slots 0 :state])}))))))
