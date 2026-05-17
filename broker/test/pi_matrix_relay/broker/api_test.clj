(ns pi-matrix-relay.broker.api-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [org.httpkit.server :as hk]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.store :as store]
            [pi-matrix-relay.broker.test-util :as tu]))

(defn with-env
  ([f]
   (with-env (tu/fake-gateway) f))
  ([gateway f]
   (let [conn (tu/test-db-conn)
         env (tu/test-env gateway conn)]
     (try
       (f env conn)
       (finally
         (db/release-conn! conn))))))

(deftest health-and-client-registration-use-json-envelope
  (testing "health and client registration expose stable v1 envelopes without an API state atom"
    (with-env
      (tu/fake-gateway)
      (fn [env conn]
        (let [app (api/app env)
              registration (tu/json-request app :post "/v1/clients"
                                            {:requestId "register-1"
                                             :instanceId "instance-1"
                                             :protocolVersion 1
                                             :project {:id "project"}
                                             :subscriptions {:rooms ["!project:example.org"]}})]
          (is (= {:health {:ok true
                           :data {:status "ok"
                                  :matrix {:connected true
                                           :userId "@bot:example.org"
                                           :encrypted true}}}
                  :registration {:ok true
                                 :data {:clientId "instance-1"
                                        :eventStream "/v1/clients/instance-1/events"
                                        :heartbeatSeconds 30
                                        :globalOperators ["@operator:example.org"]}}
                  :stored-client "instance-1"}
                 {:health (tu/response-json (tu/request app :get "/v1/health"))
                  :registration registration
                  :stored-client (:client-id (store/client @conn "instance-1"))})))))))

(deftest legacy-unencoded-client-paths-still-route-for-slashful-client-ids
  (testing "older extension builds embedded slash-containing client ids directly in route paths"
    (with-env
      (fn [env conn]
        (let [app (api/app env)
              client-id "matrix-relay-/work/project"
              raw-client-path "/v1/clients/matrix-relay-/work/project"
              _ (tu/json-request app :post "/v1/clients"
                                 {:requestId "register-legacy-path"
                                  :instanceId client-id
                                  :protocolVersion 1
                                  :project {:id "project"}})
              acquire (tu/json-request app :post "/v1/slots/acquire"
                                       {:requestId "acquire-legacy-path"
                                        :clientId client-id
                                        :project {:id "project"}})
              before (get-in (store/list-slots @conn "project") [:slots 0 :last-heartbeat-at])
              subscriptions (tu/json-request app :patch (str raw-client-path "/subscriptions")
                                             {:rooms ["!room:example.org"]})
              heartbeat (tu/json-request app :post (str raw-client-path "/heartbeat") {})
              as-channel* (atom nil)]
          (is (= {:ok true
                  :data {:slot "A" :roomId "!project-A:example.org" :roomName "project-A"}}
                 acquire))
          (with-redefs [hk/as-channel (fn [request opts]
                                        (reset! as-channel* [request opts])
                                        {:body :async-channel})
                        hk/send! (fn
                                   ([_channel _data] true)
                                   ([_channel _data _close-after-send?] true))]
            (is (= {:body :async-channel}
                   (tu/request app :get (str raw-client-path "/events"))))
            ((-> @as-channel* second :on-open) :channel))
          (is (= {:subscriptions {:ok true
                                  :data {:rooms ["!room:example.org"]}}
                  :heartbeat {:ok true
                              :data {:heartbeatSeconds 30}}
                  :last-heartbeat-advanced? true
                  :subscribers #{:channel}}
                 {:subscriptions subscriptions
                  :heartbeat heartbeat
                  :last-heartbeat-advanced? (< before (get-in (store/list-slots @conn "project")
                                                              [:slots 0 :last-heartbeat-at]))
                  :subscribers (get @(:subscribers* (:runtime env)) client-id)})))))))

(deftest event-stream-handler-returns-the-http-kit-async-response
  (testing "SSE handlers use Datahike clients/subscriptions and runtime subscriber channels"
    (with-env
      (fn [env conn]
        (let [app (api/app env)
              _ (tu/json-request app :post "/v1/clients"
                                 {:requestId "register-events"
                                  :instanceId "client-1"
                                  :protocolVersion 1
                                  :project {:id "project"}
                                  :subscriptions {:rooms ["!room:example.org"]}})
              acquire (tu/json-request app :post "/v1/slots/acquire"
                                       {:requestId "acquire-events"
                                        :clientId "client-1"
                                        :project {:id "project"}})
              _ (events/publish! env {:event "matrix.message"
                                      :data {:room {:roomId "!room:example.org"}
                                             :text "first"}})
              _ (events/publish! env {:event "matrix.message"
                                      :data {:room {:roomId "!room:example.org"}
                                             :text "second"}})
              as-channel* (atom nil)
              sends* (atom [])]
          (is (= {:ok true
                  :data {:slot "A" :roomId "!project-A:example.org" :roomName "project-A"}}
                 acquire))
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
              (is (= #{:channel} (get @(:subscribers* (:runtime env)) "client-1")))
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
              (is (= [{:slot "A" :state :suspect}]
                     (mapv #(select-keys % [:slot :state])
                           (:slots (store/list-slots @conn "project"))))))))))))

(deftest matrix-send-requires-known-room
  (testing "a client can send only to subscribed or acquired rooms"
    (with-env
      (tu/fake-gateway)
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-send"
                            :instanceId "instance-send"
                            :protocolVersion 1
                            :project {:id "project"}
                            :subscriptions {:rooms ["!project:example.org"]}})
          (is (= {:allowed {:ok true
                            :data {:roomId "!project:example.org"
                                   :eventId "$message:example.org"}}
                  :forbidden {:ok false
                              :error {:code "room_not_allowed"
                                      :message "Client is not registered for the target Matrix room."
                                      :details {:client-id "instance-send"
                                                :room-id "!other:example.org"}}}
                  :send-calls 1}
                 {:allowed (tu/json-request app :post "/v1/matrix/messages"
                                            {:requestId "send-allowed"
                                             :clientId "instance-send"
                                             :target {:roomId "!project:example.org"}
                                             :body "hello"})
                  :forbidden (tu/json-request app :post "/v1/matrix/messages"
                                              {:requestId "send-forbidden"
                                               :clientId "instance-send"
                                               :target {:roomId "!other:example.org"}
                                               :body "nope"})
                  :send-calls (count (filter #(= :send-message (first %))
                                             (tu/calls gateway)))})))))))

(deftest matrix-send-without-client-id-is-allowed-for-joined-rooms
  (testing "the local command path can send to a broker-joined room without a registered client"
    (with-env
      (fn [env _]
        (let [app (api/app env)]
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
                                               :body "nope"})})))))))

(deftest matrix-reaction-requires-known-room
  (testing "a client can react only in subscribed or acquired rooms"
    (with-env
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-reaction"
                            :instanceId "instance-reaction"
                            :protocolVersion 1
                            :project {:id "project"}
                            :subscriptions {:rooms ["!project:example.org"]}})
          (is (= {:allowed {:ok true
                            :data {:roomId "!project:example.org"
                                   :eventId "$reaction:example.org"
                                   :reactsToEventId "$message:example.org"
                                   :key "👍"}}
                  :forbidden {:ok false
                              :error {:code "room_not_allowed"
                                      :message "Client is not registered for the target Matrix room."
                                      :details {:client-id "instance-reaction"
                                                :room-id "!other:example.org"}}}
                  :reaction-calls 1}
                 {:allowed (tu/json-request app :post "/v1/matrix/reactions"
                                            {:requestId "react-allowed"
                                             :clientId "instance-reaction"
                                             :roomId "!project:example.org"
                                             :eventId "$message:example.org"
                                             :key "👍"})
                  :forbidden (tu/json-request app :post "/v1/matrix/reactions"
                                              {:requestId "react-forbidden"
                                               :clientId "instance-reaction"
                                               :roomId "!other:example.org"
                                               :eventId "$message:example.org"
                                               :key "👍"})
                  :reaction-calls (count (filter #(= :send-reaction (first %))
                                                 (tu/calls gateway)))})))))))

(deftest matrix-send-forwards-reply-target
  (testing "reply metadata reaches the Matrix gateway"
    (with-env
      (fn [env _]
        (let [gateway (:matrix-gateway env)
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
                              [:target :body :replyTo]))))))))

(deftest matrix-rooms-list-returns-joined-rooms
  (testing "broker can inspect Matrix rooms joined by the bot"
    (with-env
      (tu/fake-gateway {:rooms [{:roomId "!joined:example.org"
                                 :name "joined room"
                                 :membership "join"}
                                {:roomId "!left:example.org"
                                 :name "left room"
                                 :membership "leave"}]})
      (fn [env _]
        (let [app (api/app env)]
          (is (= {:ok true
                  :data {:rooms [{:roomId "!joined:example.org"
                                  :name "joined room"
                                  :membership "join"}]}}
                 (tu/response-json (tu/request app :get "/v1/matrix/rooms")))))))))

(deftest slot-acquire-discovers-existing-joined-room-before-creating
  (testing "empty process runtime is reconciled from joined Matrix rooms and persisted"
    (with-env
      (tu/fake-gateway {:rooms [{:roomId "!project-A-old:example.org"
                                 :name "project-A"
                                 :membership "leave"}
                                {:roomId "!project-A-existing:example.org"
                                 :name "project-A"
                                 :membership "join"}]})
      (fn [env conn]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-discover-slot"
                            :instanceId "instance-discover-slot"
                            :protocolVersion 1
                            :project {:id "project"}})
          (let [acquire (tu/json-request app :post "/v1/slots/acquire"
                                         {:requestId "acquire-discovered-slot"
                                          :clientId "instance-discover-slot"
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
                    :remembered {:room-id "!project-A-existing:example.org"
                                 :room-name "project-A"
                                 :slot "A"}}
                   {:acquire acquire
                    :created (filterv #(= :create-room (first %)) calls)
                    :ensured (mapv second (filter #(= :ensure-users-power-level (first %)) calls))
                    :remembered (select-keys (store/slot-room @conn "project" "A")
                                             [:room-id :room-name :slot])}))))))))

(deftest released-slot-room-is-reused-from-durable-db-after-runtime-loss
  (testing "slot room mappings survive process-local runtime loss through Datahike"
    (let [conn (tu/test-db-conn)]
      (try
        (let [gateway-1 (tu/fake-gateway)
              env-1 (tu/test-env gateway-1 conn)
              app-1 (api/app env-1)]
          (tu/json-request app-1 :post "/v1/clients"
                           {:requestId "register-db-slot-1"
                            :instanceId "instance-db-slot-1"
                            :protocolVersion 1
                            :project {:id "project"}})
          (is (= {:ok true
                  :data {:slot "A"
                         :roomId "!project-A:example.org"
                         :roomName "project-A"}}
                 (tu/json-request app-1 :post "/v1/slots/acquire"
                                  {:requestId "acquire-db-slot-1"
                                   :clientId "instance-db-slot-1"
                                   :project {:id "project"}})))
          (is (= {:ok true :data {:released true}}
                 (tu/json-request app-1 :post "/v1/slots/release"
                                  {:requestId "release-db-slot-1"
                                   :clientId "instance-db-slot-1"
                                   :slot "A"})))
          (let [gateway-2 (tu/fake-gateway)
                env-2 (tu/test-env gateway-2 conn)
                app-2 (api/app env-2)]
            (tu/json-request app-2 :post "/v1/clients"
                             {:requestId "register-db-slot-2"
                              :instanceId "instance-db-slot-2"
                              :protocolVersion 1
                              :project {:id "project"}})
            (is (= {:second {:ok true
                             :data {:slot "A"
                                    :roomId "!project-A:example.org"
                                    :roomName "project-A"}}
                    :first-created ["project-A"]
                    :second-created []}
                   {:second (tu/json-request app-2 :post "/v1/slots/acquire"
                                             {:requestId "acquire-db-slot-2"
                                              :clientId "instance-db-slot-2"
                                              :project {:id "project"}})
                    :first-created (mapv (comp :name second)
                                         (filter #(= :create-room (first %))
                                                 (tu/calls gateway-1)))
                    :second-created (mapv (comp :name second)
                                          (filter #(= :create-room (first %))
                                                  (tu/calls gateway-2)))}))))
        (finally
          (db/release-conn! conn))))))

(deftest active-lease-survives-process-runtime-loss-in-phase-1-store
  (testing "lease occupancy is durable even though restart-grace behavior is deferred"
    (let [conn (tu/test-db-conn)]
      (try
        (let [env-1 (tu/test-env (tu/fake-gateway) conn)
              app-1 (api/app env-1)]
          (tu/json-request app-1 :post "/v1/clients"
                           {:requestId "register-active-1"
                            :instanceId "instance-active-1"
                            :protocolVersion 1
                            :project {:id "project"}})
          (tu/json-request app-1 :post "/v1/slots/acquire"
                           {:requestId "acquire-active-1"
                            :clientId "instance-active-1"
                            :project {:id "project"}})
          (let [env-2 (tu/test-env (tu/fake-gateway) conn)
                app-2 (api/app env-2)]
            (tu/json-request app-2 :post "/v1/clients"
                             {:requestId "register-active-2"
                              :instanceId "instance-active-2"
                              :protocolVersion 1
                              :project {:id "project"}})
            (is (= {:ok true
                    :data {:slot "B"
                           :roomId "!project-B:example.org"
                           :roomName "project-B"}}
                   (tu/json-request app-2 :post "/v1/slots/acquire"
                                    {:requestId "acquire-active-2"
                                     :clientId "instance-active-2"
                                     :project {:id "project"}})))))
        (finally
          (db/release-conn! conn))))))

(deftest slot-acquire-reserves-slot-before-creating-matrix-room
  (testing "room creation side effects cannot make the assigned slot diverge from the room name"
    (let [conn (tu/test-db-conn)
          contender-id* (atom nil)
          gateway (tu/fake-gateway
                   {:on-create-room (fn [request]
                                      (when (= "project-A" (:name request))
                                        (let [reservation (store/reserve-slot!
                                                           conn
                                                           {:now-ms 1000}
                                                           {:client-id @contender-id*
                                                            :project {:id "project"}})]
                                          (store/complete-slot-reservation!
                                           conn
                                           {:now-ms 1000
                                            :lease-id (:lease-id reservation)
                                            :reservation-id (:reservation-id reservation)
                                            :client-id @contender-id*
                                            :room-id "!project-B:example.org"
                                            :room-name "project-B"}))))})]
      (try
        (let [env (tu/test-env gateway conn)
              app (api/app env)]
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-race-primary"
                            :instanceId "instance-race-primary"
                            :protocolVersion 1
                            :project {:id "project"}})
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-race-contender"
                            :instanceId "instance-race-contender"
                            :protocolVersion 1
                            :project {:id "project"}})
          (reset! contender-id* "instance-race-contender")
          (let [primary-acquire (tu/json-request app :post "/v1/slots/acquire"
                                                 {:requestId "acquire-race-primary"
                                                  :clientId "instance-race-primary"
                                                  :project {:id "project"}})]
            (is (= {:primary {:ok true
                              :data {:slot "A"
                                     :roomId "!project-A:example.org"
                                     :roomName "project-A"}}
                    :slots [{:slot "A"
                             :room-id "!project-A:example.org"
                             :client-id "instance-race-primary"
                             :state :leased}
                            {:slot "B"
                             :room-id "!project-B:example.org"
                             :client-id "instance-race-contender"
                             :state :leased}]}
                   {:primary primary-acquire
                    :slots (mapv #(select-keys % [:slot :room-id :client-id :state])
                                 (:slots (store/list-slots @conn "project")))}))))
        (finally
          (db/release-conn! conn))))))

(deftest slot-acquire-refuses-ambiguous-joined-slot-rooms
  (testing "broker does not create another room when multiple joined rooms match the slot name"
    (with-env
      (tu/fake-gateway {:rooms [{:roomId "!project-A-1:example.org"
                                 :name "project-A"
                                 :membership "join"}
                                {:roomId "!project-A-2:example.org"
                                 :name "project-A"
                                 :membership "join"}]})
      (fn [env conn]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-ambiguous-slot"
                            :instanceId "instance-ambiguous-slot"
                            :protocolVersion 1
                            :project {:id "project"}})
          (let [acquire (tu/json-request app :post "/v1/slots/acquire"
                                         {:requestId "acquire-ambiguous-slot"
                                          :clientId "instance-ambiguous-slot"
                                          :project {:id "project"}})]
            (is (= {:acquire {:ok false
                              :error {:code "slot_room_ambiguous"
                                      :message "Multiple joined Matrix rooms match the requested slot room name."
                                      :details {:project-id "project"
                                                :slot "A"
                                                :room-name "project-A"
                                                :room-ids ["!project-A-1:example.org"
                                                           "!project-A-2:example.org"]}}}
                    :created []
                    :slots []}
                   {:acquire acquire
                    :created (filterv #(= :create-room (first %)) (tu/calls gateway))
                    :slots (:slots (store/list-slots @conn "project"))}))))))))

(deftest slot-acquire-is-idempotent-and-allocates-next-free-slot
  (testing "slot leases are coordinated by Datahike and mutating requestIds are replay-safe"
    (with-env
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/json-request app :post "/v1/clients"
                           {:requestId "register-slot"
                            :instanceId "instance-slot"
                            :protocolVersion 1
                            :project {:id "project"}})
          (let [first-acquire (tu/json-request app :post "/v1/slots/acquire"
                                               {:requestId "acquire-1"
                                                :clientId "instance-slot"
                                                :project {:id "project"}
                                                :invite ["@operator:example.org"]})
                replayed-acquire (tu/json-request app :post "/v1/slots/acquire"
                                                  {:requestId "acquire-1"
                                                   :clientId "instance-slot"
                                                   :project {:id "project"}
                                                   :invite ["@operator:example.org"]})
                conflicting-replay (tu/json-request app :post "/v1/slots/acquire"
                                                    {:requestId "acquire-1"
                                                     :clientId "instance-slot"
                                                     :project {:id "other-project"}})
                second-acquire (tu/json-request app :post "/v1/slots/acquire"
                                                {:requestId "acquire-2"
                                                 :clientId "instance-slot"
                                                 :project {:id "project"}})]
            (is (= {:first {:ok true
                            :data {:slot "A"
                                   :roomId "!project-A:example.org"
                                   :roomName "project-A"}}
                    :replayed {:ok true
                               :data {:slot "A"
                                      :roomId "!project-A:example.org"
                                      :roomName "project-A"}}
                    :conflict {:ok false
                               :error {:code "idempotency_conflict"
                                       :message "Request id was reused with a different payload."
                                       :details {:request-id "acquire-1"}}}
                    :second {:ok true
                             :data {:slot "B"
                                    :roomId "!project-B:example.org"
                                    :roomName "project-B"}}
                    :create-room-names ["project-A" "project-B"]}
                   {:first first-acquire
                    :replayed replayed-acquire
                    :conflict conflicting-replay
                    :second second-acquire
                    :create-room-names (mapv (comp :name second)
                                             (filter #(= :create-room (first %))
                                                     (tu/calls gateway)))}))))))))

(deftest released-slot-room-is-reused-for-the-next-client
  (testing "slot rooms are broker-managed reusable rooms, not one Matrix room per acquire"
    (with-env
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (doseq [client-id ["instance-slot-reuse-1" "instance-slot-reuse-2"]]
            (tu/json-request app :post "/v1/clients"
                             {:requestId (str "register-" client-id)
                              :instanceId client-id
                              :protocolVersion 1
                              :project {:id "project"}}))
          (let [first-acquire (tu/json-request app :post "/v1/slots/acquire"
                                               {:requestId "acquire-reusable-1"
                                                :clientId "instance-slot-reuse-1"
                                                :project {:id "project"}})
                release (tu/json-request app :post "/v1/slots/release"
                                         {:requestId "release-reusable-1"
                                          :clientId "instance-slot-reuse-1"
                                          :slot "A"})
                second-acquire (tu/json-request app :post "/v1/slots/acquire"
                                                {:requestId "acquire-reusable-2"
                                                 :clientId "instance-slot-reuse-2"
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
                                                     (tu/calls gateway)))}))))))))
