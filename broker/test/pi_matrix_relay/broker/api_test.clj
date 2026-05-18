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

(defn path-encode
  [s]
  (-> (str s)
      (str/replace "/" "%2F")
      (str/replace ":" "%3A")))

(defn delivery-mode-path
  [client-id room-id]
  (str "/v1/clients/" (path-encode client-id)
       "/rooms/" (path-encode room-id)
       "/delivery-mode"))

(defn prompt-mode-path
  [client-id room-id]
  (str "/v1/clients/" (path-encode client-id)
       "/rooms/" (path-encode room-id)
       "/prompt-mode"))

(defn normalize-delivery-response
  [response]
  (update-in response [:data :room/default-delivery-mode-updated-at] boolean))

(defn normalize-prompt-mode-response
  [response]
  (update-in response [:data :room/prompt-mode-updated-at] boolean))

(deftest health-and-client-registration-use-edn-envelope
  (testing "health and client registration expose stable v1 envelopes without an API state atom"
    (with-env
      (tu/fake-gateway)
      (fn [env conn]
        (let [app (api/app env)
              registration (tu/edn-request app :post "/v1/clients"
                                           {:request/id "register-1"
                                            :client/instance-id "instance-1"
                                            :protocol/version 1
                                            :project {:project/id "project"}
                                            :subscriptions {:rooms ["!project:example.org"]}})]
          (is (= {:health {:ok true
                           :data {:status "ok"
                                  :matrix/connected? true
                                  :user/id "@bot:example.org"
                                  :matrix/encrypted? true}}
                  :registration {:ok true
                                 :data {:client/id "instance-1"
                                        :event-stream/path "/v1/clients/instance-1/events"
                                        :heartbeat/seconds 30
                                        :matrix/global-operators ["@operator:example.org"]}}
                  :stored-client "instance-1"}
                 {:health (tu/response-edn (tu/request app :get "/v1/health"))
                  :registration registration
                  :stored-client (:client-id (store/client @conn "instance-1"))})))))))

(deftest encoded-client-paths-route-for-slashful-client-ids
  (testing "clients percent-encode slash-containing ids in route paths"
    (with-env
      (fn [env conn]
        (let [app (api/app env)
              client-id "matrix-relay-/work/project"
              raw-client-path "/v1/clients/matrix-relay-%2Fwork%2Fproject"
              _ (tu/edn-request app :post "/v1/clients"
                                {:request/id "register-legacy-path"
                                 :client/instance-id client-id
                                 :protocol/version 1
                                 :project {:project/id "project"}})
              acquire (tu/edn-request app :post "/v1/slots/acquire"
                                      {:request/id "acquire-legacy-path"
                                       :client/id client-id
                                       :project {:project/id "project"}})
              before (get-in (store/list-slots @conn "project") [:slots 0 :last-heartbeat-at])
              subscriptions (tu/edn-request app :patch (str raw-client-path "/subscriptions")
                                            {:rooms ["!room:example.org"]})
              heartbeat (tu/edn-request app :post (str raw-client-path "/heartbeat") {})
              as-channel* (atom nil)]
          (is (= {:ok true
                  :data {:slot "A" :room/id "!project-A:example.org" :room/name "project-A"}}
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
                              :data {:heartbeat/seconds 30}}
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
              _ (tu/edn-request app :post "/v1/clients"
                                {:request/id "register-events"
                                 :client/instance-id "client-1"
                                 :protocol/version 1
                                 :project {:project/id "project"}
                                 :subscriptions {:rooms ["!room:example.org"]}})
              acquire (tu/edn-request app :post "/v1/slots/acquire"
                                      {:request/id "acquire-events"
                                       :client/id "client-1"
                                       :project {:project/id "project"}})
              _ (events/publish! env {:event "matrix.message"
                                      :data {:room/id "!room:example.org"
                                             :text "first"}})
              _ (events/publish! env {:event "matrix.message"
                                      :data {:room/id "!room:example.org"
                                             :text "second"}})
              as-channel* (atom nil)
              sends* (atom [])]
          (is (= {:ok true
                  :data {:slot "A" :room/id "!project-A:example.org" :room/name "project-A"}}
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
                            {:path-params {:client-id "client-1"}
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
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-send"
                           :client/instance-id "instance-send"
                           :protocol/version 1
                           :project {:project/id "project"}
                           :subscriptions {:rooms ["!project:example.org"]}})
          (is (= {:allowed {:ok true
                            :data {:room/id "!project:example.org"
                                   :event/id "$message:example.org"}}
                  :forbidden {:ok false
                              :error {:code "room_not_allowed"
                                      :message "Client is not registered for the target Matrix room."
                                      :details {:client-id "instance-send"
                                                :room-id "!other:example.org"}}}
                  :send-calls 1}
                 {:allowed (tu/edn-request app :post "/v1/matrix/messages"
                                           {:request/id "send-allowed"
                                            :client/id "instance-send"
                                            :target {:room/id "!project:example.org"}
                                            :body "hello"})
                  :forbidden (tu/edn-request app :post "/v1/matrix/messages"
                                             {:request/id "send-forbidden"
                                              :client/id "instance-send"
                                              :target {:room/id "!other:example.org"}
                                              :body "nope"})
                  :send-calls (count (filter #(= :send-message (first %))
                                             (tu/calls gateway)))})))))))

(deftest matrix-send-without-client-id-is-allowed-for-joined-rooms
  (testing "the local command path verifies joined-room status through the Matrix gateway"
    (with-env
      (tu/fake-gateway {:rooms [{:room/id "!joined:example.org"
                                 :room/name "joined room"
                                 :room/membership "join"}
                                {:room/id "!left:example.org"
                                 :room/name "left room"
                                 :room/membership "leave"}]})
      (fn [env _]
        (let [app (api/app env)]
          (is (= {:allowed {:ok true
                            :data {:room/id "!joined:example.org"
                                   :event/id "$message:example.org"}}
                  :forbidden {:ok false
                              :error {:code "room_not_allowed"
                                      :message "Target Matrix room is not currently joined."
                                      :details {:client-id nil
                                                :room-id "!left:example.org"}}}}
                 {:allowed (tu/edn-request app :post "/v1/matrix/messages"
                                           {:request/id "send-local-command"
                                            :target {:room/id "!joined:example.org"}
                                            :body "hello from local command"})
                  :forbidden (tu/edn-request app :post "/v1/matrix/messages"
                                             {:request/id "send-local-command-forbidden"
                                              :target {:room/id "!left:example.org"}
                                              :body "nope"})})))))))

(deftest matrix-reaction-requires-known-room
  (testing "a client can react only in subscribed or acquired rooms"
    (with-env
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-reaction"
                           :client/instance-id "instance-reaction"
                           :protocol/version 1
                           :project {:project/id "project"}
                           :subscriptions {:rooms ["!project:example.org"]}})
          (is (= {:allowed {:ok true
                            :data {:room/id "!project:example.org"
                                   :event/id "$reaction:example.org"
                                   :event/reacts-to-id "$message:example.org"
                                   :key "👍"}}
                  :forbidden {:ok false
                              :error {:code "room_not_allowed"
                                      :message "Client is not registered for the target Matrix room."
                                      :details {:client-id "instance-reaction"
                                                :room-id "!other:example.org"}}}
                  :reaction-calls 1}
                 {:allowed (tu/edn-request app :post "/v1/matrix/reactions"
                                           {:request/id "react-allowed"
                                            :client/id "instance-reaction"
                                            :room/id "!project:example.org"
                                            :event/id "$message:example.org"
                                            :key "👍"})
                  :forbidden (tu/edn-request app :post "/v1/matrix/reactions"
                                             {:request/id "react-forbidden"
                                              :client/id "instance-reaction"
                                              :room/id "!other:example.org"
                                              :event/id "$message:example.org"
                                              :key "👍"})
                  :reaction-calls (count (filter #(= :send-reaction (first %))
                                                 (tu/calls gateway)))})))))))

(deftest matrix-send-forwards-reply-target
  (testing "reply metadata reaches the Matrix gateway"
    (with-env
      (tu/fake-gateway {:rooms [{:room/id "!joined:example.org"
                                 :room/name "joined room"
                                 :room/membership "join"}]})
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/edn-request app :post "/v1/matrix/messages"
                          {:request/id "send-reply"
                           :target {:room/id "!joined:example.org"}
                           :body "reply text"
                           :reply-to {:room/id "!joined:example.org"
                                      :event/id "$parent:example.org"}})
          (is (= {:target {:room/id "!joined:example.org"}
                  :body "reply text"
                  :reply-to {:room/id "!joined:example.org"
                             :event/id "$parent:example.org"}}
                 (select-keys (second (first (filter #(= :send-message (first %))
                                                     (tu/calls gateway))))
                              [:target :body :reply-to]))))))))

(deftest matrix-rooms-list-returns-joined-rooms
  (testing "broker can inspect Matrix rooms joined by the bot"
    (with-env
      (tu/fake-gateway {:rooms [{:room/id "!joined:example.org"
                                 :room/name "joined room"
                                 :room/membership "join"}
                                {:room/id "!left:example.org"
                                 :room/name "left room"
                                 :room/membership "leave"}]})
      (fn [env conn]
        (let [app (api/app env)]
          (store/ensure-room! conn "!only-in-db:example.org")
          (is (= {:ok true
                  :data {:rooms [{:room/id "!joined:example.org"
                                  :room/name "joined room"
                                  :room/membership "join"}]}}
                 (tu/response-edn (tu/request app :get "/v1/matrix/rooms")))))))))

(deftest room-delivery-mode-endpoints-are-client-scoped-and-persistent
  (testing "clients can read and write default room delivery mode only for known rooms"
    (with-env
      (fn [env _]
        (let [app (api/app env)]
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-delivery-mode-1"
                           :client/instance-id "matrix-relay-/work/project"
                           :protocol/version 1
                           :project {:project/id "project"}
                           :subscriptions {:rooms ["!project:example.org"]}})
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-delivery-mode-2"
                           :client/instance-id "client-slot"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (let [slot (tu/edn-request app :post "/v1/slots/acquire"
                                     {:request/id "acquire-delivery-slot"
                                      :client/id "client-slot"
                                      :project {:project/id "project"}})
                project-path (delivery-mode-path "matrix-relay-/work/project" "!project:example.org")
                slot-path (delivery-mode-path "client-slot" (get-in slot [:data :room/id]))]
            (is (= {:before {:ok true
                             :data {:room/id "!project:example.org"
                                    :room/default-delivery-mode nil
                                    :room/default-delivery-mode-updated-at false
                                    :room/default-delivery-mode-updated-by-client nil
                                    :room/default-delivery-mode-updated-by-user nil}}
                    :write {:ok true
                            :data {:room/id "!project:example.org"
                                   :room/default-delivery-mode "steer"
                                   :room/default-delivery-mode-updated-at true
                                   :room/default-delivery-mode-updated-by-client "matrix-relay-/work/project"
                                   :room/default-delivery-mode-updated-by-user "@alice:example.org"}}
                    :after {:ok true
                            :data {:room/id "!project:example.org"
                                   :room/default-delivery-mode "steer"
                                   :room/default-delivery-mode-updated-at true
                                   :room/default-delivery-mode-updated-by-client "matrix-relay-/work/project"
                                   :room/default-delivery-mode-updated-by-user "@alice:example.org"}}
                    :slot-write {:ok true
                                 :data {:room/id "!project-A:example.org"
                                        :room/default-delivery-mode "reject"
                                        :room/default-delivery-mode-updated-at true
                                        :room/default-delivery-mode-updated-by-client "client-slot"
                                        :room/default-delivery-mode-updated-by-user "@bob:example.org"}}}
                   {:before (normalize-delivery-response (tu/edn-request app :get project-path nil))
                    :write (normalize-delivery-response
                            (tu/edn-request app :put project-path
                                            {:room/default-delivery-mode "steer"
                                             :room/default-delivery-mode-updated-by-user "@alice:example.org"}))
                    :after (normalize-delivery-response (tu/edn-request app :get project-path nil))
                    :slot-write (normalize-delivery-response
                                 (tu/edn-request app :put slot-path
                                                 {:room/default-delivery-mode "reject"
                                                  :room/default-delivery-mode-updated-by-user "@bob:example.org"}))}))))))))

(deftest room-prompt-mode-endpoints-are-client-scoped-and-persistent
  (with-env
    (fn [env _]
      (let [app (api/app env)]
        (tu/edn-request app :post "/v1/clients"
                        {:request/id "register-prompt-mode"
                         :client/instance-id "client-1"
                         :protocol/version 1
                         :project {:project/id "project"}
                         :subscriptions {:rooms ["!project:example.org"]}})
        (let [path (prompt-mode-path "client-1" "!project:example.org")]
          (is (= {:before {:ok true
                           :data {:room/id "!project:example.org"
                                  :room/prompt-mode nil
                                  :room/prompt-mode-updated-at false
                                  :room/prompt-mode-updated-by-client nil
                                  :room/prompt-mode-updated-by-user nil}}
                  :write {:ok true
                          :data {:room/id "!project:example.org"
                                 :room/prompt-mode "commands-only"
                                 :room/prompt-mode-updated-at true
                                 :room/prompt-mode-updated-by-client "client-1"
                                 :room/prompt-mode-updated-by-user "@alice:example.org"}}
                  :after {:ok true
                          :data {:room/id "!project:example.org"
                                 :room/prompt-mode "commands-only"
                                 :room/prompt-mode-updated-at true
                                 :room/prompt-mode-updated-by-client "client-1"
                                 :room/prompt-mode-updated-by-user "@alice:example.org"}}}
                 {:before (normalize-prompt-mode-response (tu/edn-request app :get path nil))
                  :write (normalize-prompt-mode-response
                          (tu/edn-request app :put path
                                          {:room/prompt-mode "commands-only"
                                           :room/prompt-mode-updated-by-user "@alice:example.org"}))
                  :after (normalize-prompt-mode-response (tu/edn-request app :get path nil))})))))))

(deftest room-prompt-mode-endpoints-reject-invalid-or-unauthorized-requests
  (with-env
    (fn [env _]
      (let [app (api/app env)
            project-path (prompt-mode-path "client-1" "!project:example.org")
            other-path (prompt-mode-path "client-1" "!other:example.org")]
        (tu/edn-request app :post "/v1/clients"
                        {:request/id "register-prompt-mode-rejects"
                         :client/instance-id "client-1"
                         :protocol/version 1
                         :project {:project/id "project"}
                         :subscriptions {:rooms ["!project:example.org"]}})
        (is (= {:unknown {:ok false
                          :error {:code "client_not_found"
                                  :message "Unknown broker client."
                                  :details {:client-id "missing-client"}}}
                :unauthorized {:ok false
                               :error {:code "room_not_allowed"
                                       :message "Client is not registered for the target Matrix room."
                                       :details {:client-id "client-1"
                                                 :room-id "!other:example.org"}}}
                :invalid {:ok false
                          :error {:code "invalid_request"
                                  :message "Invalid prompt mode."
                                  :details {:room/prompt-mode "interrupt"
                                            :allowed ["all" "commands-only" "mentions"]}}}}
               {:unknown (tu/edn-request app :get
                                         (prompt-mode-path "missing-client" "!project:example.org")
                                         nil)
                :unauthorized (tu/edn-request app :put other-path
                                              {:room/prompt-mode "all"
                                               :room/prompt-mode-updated-by-user "@alice:example.org"})
                :invalid (tu/edn-request app :put project-path
                                         {:room/prompt-mode "interrupt"
                                          :room/prompt-mode-updated-by-user "@alice:example.org"})}))))))

(deftest room-delivery-mode-endpoints-reject-invalid-or-unauthorized-requests
  (with-env
    (fn [env _]
      (let [app (api/app env)
            project-path (delivery-mode-path "client-1" "!project:example.org")
            other-path (delivery-mode-path "client-1" "!other:example.org")]
        (tu/edn-request app :post "/v1/clients"
                        {:request/id "register-delivery-mode-rejects"
                         :client/instance-id "client-1"
                         :protocol/version 1
                         :project {:project/id "project"}
                         :subscriptions {:rooms ["!project:example.org"]}})
        (is (= {:unknown {:ok false
                          :error {:code "client_not_found"
                                  :message "Unknown broker client."
                                  :details {:client-id "missing-client"}}}
                :unauthorized {:ok false
                               :error {:code "room_not_allowed"
                                       :message "Client is not registered for the target Matrix room."
                                       :details {:client-id "client-1"
                                                 :room-id "!other:example.org"}}}
                :invalid {:ok false
                          :error {:code "invalid_request"
                                  :message "Invalid room delivery mode."
                                  :details {:default-delivery-mode "interrupt"
                                            :allowed ["follow-up" "reject" "steer"]}}}}
               {:unknown (tu/edn-request app :get
                                         (delivery-mode-path "missing-client" "!project:example.org")
                                         nil)
                :unauthorized (tu/edn-request app :put other-path
                                              {:room/default-delivery-mode "steer"
                                               :room/default-delivery-mode-updated-by-user "@alice:example.org"})
                :invalid (tu/edn-request app :put project-path
                                         {:room/default-delivery-mode "interrupt"
                                          :room/default-delivery-mode-updated-by-user "@alice:example.org"})}))))))

(deftest slot-acquire-discovers-existing-joined-room-before-creating
  (testing "empty process runtime is reconciled from joined Matrix rooms and persisted"
    (with-env
      (tu/fake-gateway {:rooms [{:room/id "!project-A-old:example.org"
                                 :room/name "project-A"
                                 :room/membership "leave"}
                                {:room/id "!project-A-existing:example.org"
                                 :room/name "project-A"
                                 :room/membership "join"}]})
      (fn [env conn]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-discover-slot"
                           :client/instance-id "instance-discover-slot"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (let [acquire (tu/edn-request app :post "/v1/slots/acquire"
                                        {:request/id "acquire-discovered-slot"
                                         :client/id "instance-discover-slot"
                                         :project {:project/id "project"}
                                         :invite ["@operator:example.org"]})
                calls (tu/calls gateway)]
            (is (= {:acquire {:ok true
                              :data {:slot "A"
                                     :room/id "!project-A-existing:example.org"
                                     :room/name "project-A"}}
                    :created []
                    :ensured [{:room/id "!project-A-existing:example.org"
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
          (tu/edn-request app-1 :post "/v1/clients"
                          {:request/id "register-db-slot-1"
                           :client/instance-id "instance-db-slot-1"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (is (= {:ok true
                  :data {:slot "A"
                         :room/id "!project-A:example.org"
                         :room/name "project-A"}}
                 (tu/edn-request app-1 :post "/v1/slots/acquire"
                                 {:request/id "acquire-db-slot-1"
                                  :client/id "instance-db-slot-1"
                                  :project {:project/id "project"}})))
          (is (= {:ok true :data {:released true}}
                 (tu/edn-request app-1 :post "/v1/slots/release"
                                 {:request/id "release-db-slot-1"
                                  :client/id "instance-db-slot-1"
                                  :slot "A"})))
          (let [gateway-2 (tu/fake-gateway)
                env-2 (tu/test-env gateway-2 conn)
                app-2 (api/app env-2)]
            (tu/edn-request app-2 :post "/v1/clients"
                            {:request/id "register-db-slot-2"
                             :client/instance-id "instance-db-slot-2"
                             :protocol/version 1
                             :project {:project/id "project"}})
            (is (= {:second {:ok true
                             :data {:slot "A"
                                    :room/id "!project-A:example.org"
                                    :room/name "project-A"}}
                    :first-created ["project-A"]
                    :second-created []}
                   {:second (tu/edn-request app-2 :post "/v1/slots/acquire"
                                            {:request/id "acquire-db-slot-2"
                                             :client/id "instance-db-slot-2"
                                             :project {:project/id "project"}})
                    :first-created (mapv (comp :room/name second)
                                         (filter #(= :create-room (first %))
                                                 (tu/calls gateway-1)))
                    :second-created (mapv (comp :room/name second)
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
          (tu/edn-request app-1 :post "/v1/clients"
                          {:request/id "register-active-1"
                           :client/instance-id "instance-active-1"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (tu/edn-request app-1 :post "/v1/slots/acquire"
                          {:request/id "acquire-active-1"
                           :client/id "instance-active-1"
                           :project {:project/id "project"}})
          (let [env-2 (tu/test-env (tu/fake-gateway) conn)
                app-2 (api/app env-2)]
            (tu/edn-request app-2 :post "/v1/clients"
                            {:request/id "register-active-2"
                             :client/instance-id "instance-active-2"
                             :protocol/version 1
                             :project {:project/id "project"}})
            (is (= {:ok true
                    :data {:slot "B"
                           :room/id "!project-B:example.org"
                           :room/name "project-B"}}
                   (tu/edn-request app-2 :post "/v1/slots/acquire"
                                   {:request/id "acquire-active-2"
                                    :client/id "instance-active-2"
                                    :project {:project/id "project"}})))))
        (finally
          (db/release-conn! conn))))))

(deftest slot-acquire-reserves-slot-before-creating-matrix-room
  (testing "room creation side effects cannot make the assigned slot diverge from the room name"
    (let [conn (tu/test-db-conn)
          contender-id* (atom nil)
          gateway (tu/fake-gateway
                   {:on-create-room (fn [request]
                                      (when (= "project-A" (:room/name request))
                                        (let [reservation (store/reserve-slot!
                                                           conn
                                                           {:now-ms 1000}
                                                           {:client-id @contender-id*
                                                            :project {:project/id "project"}})]
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
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-race-primary"
                           :client/instance-id "instance-race-primary"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-race-contender"
                           :client/instance-id "instance-race-contender"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (reset! contender-id* "instance-race-contender")
          (let [primary-acquire (tu/edn-request app :post "/v1/slots/acquire"
                                                {:request/id "acquire-race-primary"
                                                 :client/id "instance-race-primary"
                                                 :project {:project/id "project"}})]
            (is (= {:primary {:ok true
                              :data {:slot "A"
                                     :room/id "!project-A:example.org"
                                     :room/name "project-A"}}
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
      (tu/fake-gateway {:rooms [{:room/id "!project-A-1:example.org"
                                 :room/name "project-A"
                                 :room/membership "join"}
                                {:room/id "!project-A-2:example.org"
                                 :room/name "project-A"
                                 :room/membership "join"}]})
      (fn [env conn]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-ambiguous-slot"
                           :client/instance-id "instance-ambiguous-slot"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (let [acquire (tu/edn-request app :post "/v1/slots/acquire"
                                        {:request/id "acquire-ambiguous-slot"
                                         :client/id "instance-ambiguous-slot"
                                         :project {:project/id "project"}})]
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
          (tu/edn-request app :post "/v1/clients"
                          {:request/id "register-slot"
                           :client/instance-id "instance-slot"
                           :protocol/version 1
                           :project {:project/id "project"}})
          (let [first-acquire (tu/edn-request app :post "/v1/slots/acquire"
                                              {:request/id "acquire-1"
                                               :client/id "instance-slot"
                                               :project {:project/id "project"}
                                               :invite ["@operator:example.org"]})
                replayed-acquire (tu/edn-request app :post "/v1/slots/acquire"
                                                 {:request/id "acquire-1"
                                                  :client/id "instance-slot"
                                                  :project {:project/id "project"}
                                                  :invite ["@operator:example.org"]})
                conflicting-replay (tu/edn-request app :post "/v1/slots/acquire"
                                                   {:request/id "acquire-1"
                                                    :client/id "instance-slot"
                                                    :project {:project/id "other-project"}})
                second-acquire (tu/edn-request app :post "/v1/slots/acquire"
                                               {:request/id "acquire-2"
                                                :client/id "instance-slot"
                                                :project {:project/id "project"}})]
            (is (= {:first {:ok true
                            :data {:slot "A"
                                   :room/id "!project-A:example.org"
                                   :room/name "project-A"}}
                    :replayed {:ok true
                               :data {:slot "A"
                                      :room/id "!project-A:example.org"
                                      :room/name "project-A"}}
                    :conflict {:ok false
                               :error {:code "idempotency_conflict"
                                       :message "Request id was reused with a different payload."
                                       :details {:request-id "acquire-1"}}}
                    :second {:ok true
                             :data {:slot "B"
                                    :room/id "!project-B:example.org"
                                    :room/name "project-B"}}
                    :create-room-names ["project-A" "project-B"]}
                   {:first first-acquire
                    :replayed replayed-acquire
                    :conflict conflicting-replay
                    :second second-acquire
                    :create-room-names (mapv (comp :room/name second)
                                             (filter #(= :create-room (first %))
                                                     (tu/calls gateway)))}))))))))

(deftest released-slot-room-is-reused-for-the-next-client
  (testing "slot rooms are broker-managed reusable rooms, not one Matrix room per acquire"
    (with-env
      (fn [env _]
        (let [gateway (:matrix-gateway env)
              app (api/app env)]
          (doseq [client-id ["instance-slot-reuse-1" "instance-slot-reuse-2"]]
            (tu/edn-request app :post "/v1/clients"
                            {:request/id (str "register-" client-id)
                             :client/instance-id client-id
                             :protocol/version 1
                             :project {:project/id "project"}}))
          (let [first-acquire (tu/edn-request app :post "/v1/slots/acquire"
                                              {:request/id "acquire-reusable-1"
                                               :client/id "instance-slot-reuse-1"
                                               :project {:project/id "project"}})
                release (tu/edn-request app :post "/v1/slots/release"
                                        {:request/id "release-reusable-1"
                                         :client/id "instance-slot-reuse-1"
                                         :slot "A"})
                second-acquire (tu/edn-request app :post "/v1/slots/acquire"
                                               {:request/id "acquire-reusable-2"
                                                :client/id "instance-slot-reuse-2"
                                                :project {:project/id "project"}})]
            (is (= {:first {:ok true
                            :data {:slot "A"
                                   :room/id "!project-A:example.org"
                                   :room/name "project-A"}}
                    :release {:ok true
                              :data {:released true}}
                    :second {:ok true
                             :data {:slot "A"
                                    :room/id "!project-A:example.org"
                                    :room/name "project-A"}}
                    :create-room-names ["project-A"]}
                   {:first first-acquire
                    :release release
                    :second second-acquire
                    :create-room-names (mapv (comp :room/name second)
                                             (filter #(= :create-room (first %))
                                                     (tu/calls gateway)))}))))))))