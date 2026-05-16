(ns pi-matrix-relay.broker.api-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [org.httpkit.server :as hk]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.state :as state]
            [pi-matrix-relay.broker.test-util :as tu]))

(deftest health-and-client-registration-use-json-envelope
  (testing "health and client registration expose stable v1 envelopes"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)
          registration (tu/json-request app :post "/v1/clients"
                                        {:requestId "register-1"
                                         :clientInstanceId "instance-1"
                                         :protocolVersion 1
                                         :project {:id "project"}
                                         :subscriptions {:rooms ["!project:example.org"]}})]
      (is (= {:health {:ok true
                       :data {:status "ok"
                              :matrix {:connected true
                                       :userId "@bot:example.org"
                                       :encrypted true}}}
              :registration {:ok true
                             :data {:eventStream (str "/v1/clients/" (get-in registration [:data :clientId]) "/events")
                                    :heartbeatSeconds 30
                                    :globalOperators ["@operator:example.org"]}}
              :client-count 1}
             {:health (tu/response-json (tu/request app :get "/v1/health"))
              :registration (update-in registration [:data] dissoc :clientId)
              :client-count (count (:clients @(:state* env)))})))))

(deftest event-stream-handler-returns-the-http-kit-async-response
  (testing "SSE handlers must return hk/as-channel's {:body channel} response"
    (let [env (tu/test-env)
          state* (:state* env)
          _ (state/register-client! state*
                                    {:client-id-fn (constantly "client-1")
                                     :heartbeat-seconds 30
                                     :global-operators []}
                                    {:subscriptions {:rooms ["!room:example.org"]}})
          _ (state/append-event! state* {:event "matrix.message"
                                         :data {:room {:roomId "!room:example.org"}
                                                :text "first"}})
          _ (state/append-event! state* {:event "matrix.message"
                                         :data {:room {:roomId "!room:example.org"}
                                                :text "second"}})
          as-channel* (atom nil)
          sends* (atom [])]
      (with-redefs [hk/as-channel (fn [request opts]
                                    (reset! as-channel* [request opts])
                                    {:body :async-channel})
                    hk/send! (fn
                               ([channel data]
                                (hk/send! channel data true))
                               ([channel data close-after-send?]
                                (swap! sends* conj [channel data close-after-send?])
                                true))]
        (let [response ((api/event-stream-handler env)
                        {:path-params {:clientId "client-1"}
                         :headers {"last-event-id" "evt-0"}})
              on-open (-> @as-channel* second :on-open)]
          (is (= {:body :async-channel} response))
          (on-open :channel)
          (is (= #{:channel} (get @(:subscribers* env) "client-1")))
          (is (some (fn [[channel payload close?]]
                      (and (= :channel channel)
                           (map? payload)
                           (= ": connected\n\n" (:body payload))
                           (false? close?)))
                    @sends*))
          (is (some (fn [[channel payload close?]]
                      (and (= :channel channel)
                           (string? payload)
                           (str/includes? payload "id: evt-1")
                           (false? close?)))
                    @sends*)))))))

(deftest matrix-send-requires-known-room
  (testing "a client can send only to subscribed or acquired rooms"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)
          registration (tu/json-request app :post "/v1/clients"
                                        {:requestId "register-send"
                                         :clientInstanceId "instance-send"
                                         :protocolVersion 1
                                         :project {:id "project"}
                                         :subscriptions {:rooms ["!project:example.org"]}})
          client-id (get-in registration [:data :clientId])]
      (is (= {:allowed {:ok true
                        :data {:roomId "!project:example.org"
                               :eventId "$message:example.org"}}
              :forbidden {:ok false
                          :error {:code "room_not_allowed"
                                  :message "Client is not registered for the target Matrix room."
                                  :details {:client-id client-id
                                            :room-id "!other:example.org"}}}
              :send-calls 1}
             {:allowed (tu/json-request app :post "/v1/matrix/messages"
                                       {:requestId "send-allowed"
                                        :clientId client-id
                                        :target {:roomId "!project:example.org"}
                                        :body "hello"})
              :forbidden (tu/json-request app :post "/v1/matrix/messages"
                                         {:requestId "send-forbidden"
                                          :clientId client-id
                                          :target {:roomId "!other:example.org"}
                                          :body "nope"})
              :send-calls (count (filter #(= :send-message (first %))
                                         (tu/calls gateway)))})))))

(deftest matrix-send-without-client-id-is-allowed-for-joined-rooms
  (testing "the local command spike can send to a broker-joined room over UDS without a registered client"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)]
      (tu/json-request app :post "/v1/matrix/rooms/resolve"
                       {:requestId "resolve-local-command"
                        :room "!joined:example.org"})
      (is (= {:allowed {:ok true
                        :data {:roomId "!joined:example.org"
                               :eventId "$message:example.org"}}
              :forbidden {:ok false
                          :error {:code "room_not_allowed"
                                  :message "Target Matrix room has not been joined or registered for this client."
                                  :details {:client-id nil
                                            :room-id "!missing:example.org"}}}}
             {:allowed (tu/json-request app :post "/v1/matrix/messages"
                                       {:requestId "send-local-command"
                                        :target {:roomId "!joined:example.org"}
                                        :body "hello from local command"})
              :forbidden (tu/json-request app :post "/v1/matrix/messages"
                                         {:requestId "send-local-command-forbidden"
                                          :target {:roomId "!missing:example.org"}
                                          :body "nope"})})))))

(deftest matrix-reaction-requires-known-room
  (testing "a client can react only in subscribed or acquired rooms"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)
          registration (tu/json-request app :post "/v1/clients"
                                        {:requestId "register-reaction"
                                         :clientInstanceId "instance-reaction"
                                         :protocolVersion 1
                                         :project {:id "project"}
                                         :subscriptions {:rooms ["!project:example.org"]}})
          client-id (get-in registration [:data :clientId])]
      (is (= {:allowed {:ok true
                        :data {:roomId "!project:example.org"
                               :eventId "$reaction:example.org"
                               :reactsToEventId "$message:example.org"
                               :key "👍"}}
              :forbidden {:ok false
                          :error {:code "room_not_allowed"
                                  :message "Client is not registered for the target Matrix room."
                                  :details {:client-id client-id
                                            :room-id "!other:example.org"}}}
              :reaction-calls 1}
             {:allowed (tu/json-request app :post "/v1/matrix/reactions"
                                       {:requestId "react-allowed"
                                        :clientId client-id
                                        :roomId "!project:example.org"
                                        :eventId "$message:example.org"
                                        :key "👍"})
              :forbidden (tu/json-request app :post "/v1/matrix/reactions"
                                         {:requestId "react-forbidden"
                                          :clientId client-id
                                          :roomId "!other:example.org"
                                          :eventId "$message:example.org"
                                          :key "👍"})
              :reaction-calls (count (filter #(= :send-reaction (first %))
                                             (tu/calls gateway)))})))))

(deftest matrix-send-forwards-reply-target
  (testing "reply metadata reaches the Matrix gateway"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)]
      (tu/json-request app :post "/v1/matrix/rooms/resolve"
                       {:requestId "resolve-reply"
                        :room "!joined:example.org"})
      (tu/json-request app :post "/v1/matrix/messages"
                       {:requestId "send-reply"
                        :target {:roomId "!joined:example.org"}
                        :body "reply text"
                        :replyTo {:roomId "!joined:example.org"
                                  :eventId "$parent:example.org"}})
      (is (= {:target {:roomId "!joined:example.org"}
              :body "reply text"
              :replyTo {:roomId "!joined:example.org"
                        :eventId "$parent:example.org"}}
             (select-keys (second (first (filter #(= :send-message (first %))
                                                 (tu/calls gateway))))
                          [:target :body :replyTo]))))))

(deftest slot-acquire-is-idempotent-and-allocates-next-free-slot
  (testing "slot leases are coordinated by the broker and mutating requestIds are replay-safe"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)
          client-id (get-in (tu/json-request app :post "/v1/clients"
                                             {:requestId "register-slot"
                                              :clientInstanceId "instance-slot"
                                              :protocolVersion 1
                                              :project {:id "project"}})
                            [:data :clientId])
          first-acquire (tu/json-request app :post "/v1/slots/acquire"
                                         {:requestId "acquire-1"
                                          :clientId client-id
                                          :project {:id "project"}
                                          :invite ["@operator:example.org"]})
          replayed-acquire (tu/json-request app :post "/v1/slots/acquire"
                                            {:requestId "acquire-1"
                                             :clientId client-id
                                             :project {:id "project"}
                                             :invite ["@operator:example.org"]})
          second-acquire (tu/json-request app :post "/v1/slots/acquire"
                                          {:requestId "acquire-2"
                                           :clientId client-id
                                           :project {:id "project"}})]
      (is (= {:first {:ok true
                      :data {:slot "A"
                             :roomId "!project-A:example.org"
                             :roomName "project-A"}}
              :replayed {:ok true
                         :data {:slot "A"
                                :roomId "!project-A:example.org"
                                :roomName "project-A"}}
              :second {:ok true
                       :data {:slot "B"
                              :roomId "!project-B:example.org"
                              :roomName "project-B"}}
              :create-room-names ["project-A" "project-B"]}
             {:first first-acquire
              :replayed replayed-acquire
              :second second-acquire
              :create-room-names (mapv (comp :name second)
                                       (filter #(= :create-room (first %))
                                               (tu/calls gateway)))})))))