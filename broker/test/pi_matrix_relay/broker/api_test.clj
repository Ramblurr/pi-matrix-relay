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
          _ (state/acquire-slot! state*
                                 {:now 1000}
                                 {:client-id "client-1"
                                  :project {:id "project"}
                                  :room-id "!slot:example.org"
                                  :room-name "project-A"})
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
                    @sends*))
          ((-> @as-channel* second :on-close) :channel nil)
          (is (= [{:slot "A" :state "suspect"}]
                 (mapv #(select-keys % [:slot :state])
                       (:slots (state/list-slots @state* "project"))))))))))

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

(deftest matrix-rooms-list-returns-joined-rooms
  (testing "broker can inspect Matrix rooms joined by the bot"
    (let [gateway (tu/fake-gateway {:rooms [{:roomId "!joined:example.org"
                                             :name "joined room"
                                             :membership "join"}
                                            {:roomId "!left:example.org"
                                             :name "left room"
                                             :membership "leave"}]})
          env (tu/test-env gateway)
          app (api/app env)]
      (is (= {:ok true
              :data {:rooms [{:roomId "!joined:example.org"
                              :name "joined room"
                              :membership "join"}]}}
             (tu/response-json (tu/request app :get "/v1/matrix/rooms")))))))

(deftest slot-acquire-discovers-existing-joined-room-before-creating
  (testing "empty in-memory slot-room state is reconciled from joined Matrix rooms"
    (let [gateway (tu/fake-gateway {:rooms [{:roomId "!project-A-old:example.org"
                                             :name "project-A"
                                             :membership "leave"}
                                            {:roomId "!project-A-existing:example.org"
                                             :name "project-A"
                                             :membership "join"}]})
          env (tu/test-env gateway)
          app (api/app env)
          client-id (get-in (tu/json-request app :post "/v1/clients"
                                             {:requestId "register-discover-slot"
                                              :clientInstanceId "instance-discover-slot"
                                              :protocolVersion 1
                                              :project {:id "project"}})
                            [:data :clientId])
          acquire (tu/json-request app :post "/v1/slots/acquire"
                                   {:requestId "acquire-discovered-slot"
                                    :clientId client-id
                                    :project {:id "project"}
                                    :invite ["@operator:example.org"]})
          calls (tu/calls gateway)]
      (is (= {:acquire {:ok true
                        :data {:slot "A"
                               :roomId "!project-A-existing:example.org"
                               :roomName "project-A"}}
              :created []
              :ensured [{:roomId "!project-A-existing:example.org"
                         :users ["@operator:example.org"]
                         :level 100}]
              :remembered {:roomId "!project-A-existing:example.org"
                           :name "project-A"
                           :project-id "project"
                           :slot "A"}}
             {:acquire acquire
              :created (filterv #(= :create-room (first %)) calls)
              :ensured (mapv second (filter #(= :ensure-users-power-level (first %)) calls))
              :remembered (get-in @(:state* env) [:slot-rooms "project" "A"])})))))

(deftest slot-acquire-refuses-ambiguous-joined-slot-rooms
  (testing "broker does not create a fourth room when multiple joined rooms match the slot name"
    (let [gateway (tu/fake-gateway {:rooms [{:roomId "!project-A-1:example.org"
                                             :name "project-A"
                                             :membership "join"}
                                            {:roomId "!project-A-2:example.org"
                                             :name "project-A"
                                             :membership "join"}]})
          env (tu/test-env gateway)
          app (api/app env)
          client-id (get-in (tu/json-request app :post "/v1/clients"
                                             {:requestId "register-ambiguous-slot"
                                              :clientInstanceId "instance-ambiguous-slot"
                                              :protocolVersion 1
                                              :project {:id "project"}})
                            [:data :clientId])
          acquire (tu/json-request app :post "/v1/slots/acquire"
                                   {:requestId "acquire-ambiguous-slot"
                                    :clientId client-id
                                    :project {:id "project"}})]
      (is (= {:acquire {:ok false
                        :error {:code "slot_room_ambiguous"
                                :message "Multiple joined Matrix rooms match the requested slot room name."
                                :details {:project-id "project"
                                          :slot "A"
                                          :room-name "project-A"
                                          :room-ids ["!project-A-1:example.org"
                                                     "!project-A-2:example.org"]}}}
              :created []}
             {:acquire acquire
              :created (filterv #(= :create-room (first %)) (tu/calls gateway))})))))

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

(deftest released-slot-room-is-reused-for-the-next-client
  (testing "slot rooms are broker-managed reusable rooms, not one Matrix room per acquire"
    (let [gateway (tu/fake-gateway)
          env (tu/test-env gateway)
          app (api/app env)
          client-1 (get-in (tu/json-request app :post "/v1/clients"
                                            {:requestId "register-slot-reuse-1"
                                             :clientInstanceId "instance-slot-reuse-1"
                                             :protocolVersion 1
                                             :project {:id "project"}})
                           [:data :clientId])
          client-2 (get-in (tu/json-request app :post "/v1/clients"
                                            {:requestId "register-slot-reuse-2"
                                             :clientInstanceId "instance-slot-reuse-2"
                                             :protocolVersion 1
                                             :project {:id "project"}})
                           [:data :clientId])
          first-acquire (tu/json-request app :post "/v1/slots/acquire"
                                         {:requestId "acquire-reusable-1"
                                          :clientId client-1
                                          :project {:id "project"}})
          release (tu/json-request app :post "/v1/slots/release"
                                   {:requestId "release-reusable-1"
                                    :clientId client-1
                                    :slot "A"})
          second-acquire (tu/json-request app :post "/v1/slots/acquire"
                                          {:requestId "acquire-reusable-2"
                                           :clientId client-2
                                           :project {:id "project"}})]
      (is (= {:first {:ok true
                      :data {:slot "A"
                             :roomId "!project-A:example.org"
                             :roomName "project-A"}}
              :release {:ok true
                        :data {:released true}}
              :second {:ok true
                       :data {:slot "A"
                              :roomId "!project-A:example.org"
                              :roomName "project-A"}}
              :create-room-names ["project-A"]}
             {:first first-acquire
              :release release
              :second second-acquire
              :create-room-names (mapv (comp :name second)
                                       (filter #(= :create-room (first %))
                                               (tu/calls gateway)))})))))
