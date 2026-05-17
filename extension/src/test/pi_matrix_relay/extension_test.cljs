(ns pi-matrix-relay.extension-test
  (:require [cljs.test :refer [async deftest is testing]]
            [clojure.string :as str]
            [pi-matrix-relay.config :as config]
            [pi-matrix-relay.extension :as extension]))

(deftest greeting-includes-target
  (testing "the extension test runner can load project namespaces"
    (is (= "Hello, Matrix, from ClojureScript!"
           (extension/greeting "Matrix")))))

(deftest registers-long-and-short-commands-and-send-tool
  (let [commands* (atom {})
        tools* (atom {})
        active-tools* (atom ["read" "bash"])
        pi #js {:registerCommand (fn [name opts]
                                   (swap! commands* assoc name opts))
                :registerTool (fn [tool]
                                (swap! tools* assoc (.-name tool) tool))
                :getActiveTools (fn []
                                  (clj->js @active-tools*))
                :setActiveTools (fn [tools]
                                  (reset! active-tools* (js->clj tools)))}]
    (extension/init pi)
    (is (= #{"matrix-relay" "mr"}
           (set (keys @commands*))))
    (is (= #{"send_matrix_message" "send_matrix_reaction"
             "matrix_relay_diagnostics" "matrix_relay_control"}
           (set (keys @tools*))))
    (is (= ["read" "bash"] @active-tools*)
        "extension load must not call runtime action methods such as setActiveTools")
    (is (fn? (.-execute ^js (get @tools* "send_matrix_message"))))
    (is (fn? (.-execute ^js (get @tools* "send_matrix_reaction"))))
    (is (fn? (.-execute ^js (get @tools* "matrix_relay_diagnostics"))))
    (is (fn? (.-execute ^js (get @tools* "matrix_relay_control"))))))

(deftest activates-relay-tools-after-session-start
  (let [events* (atom {})
        active-tools* (atom ["read" "bash"])
        deps (assoc extension/default-deps
                    :start-relay! (fn [_deps _pi _ctx]
                                    (js/Promise.resolve nil)))
        pi #js {:registerCommand (fn [_name _opts])
                :registerTool (fn [_tool])
                :getActiveTools (fn []
                                  (clj->js @active-tools*))
                :setActiveTools (fn [tools]
                                  (reset! active-tools* (js->clj tools)))
                :on (fn [event handler]
                      (swap! events* assoc event handler))}]
    (extension/init pi deps)
    (is (= ["read" "bash"] @active-tools*))
    ((get @events* "session_start") #js {} #js {})
    (is (every? (set @active-tools*) extension/relay-tool-names))
    (is (contains? (set @active-tools*) "read"))))

(deftest room-bind-resolves-room-and-writes-project-config
  (async done
    (let [notifications* (atom [])
          written* (atom nil)
          deps {:resolve-room! (fn [room]
                                 (js/Promise.resolve {:roomId "!room:example.org"
                                                      :canonicalAlias room
                                                      :name "Pi Room"}))
                :read-project-config! (fn [_cwd] {})
                :write-project-config! (fn [cwd config]
                                        (reset! written* {:cwd cwd :config config}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "room bind #pi:example.org ops" ctx)
          (.then (fn [_]
                   (is (= {:cwd "/work/project"
                           :config {:rooms {"ops" {:alias "ops"
                                                   :roomId "!room:example.org"
                                                   :canonicalAlias "#pi:example.org"
                                                   :name "Pi Room"
                                                   :mode "mentions"
                                                   :busy "follow-up"}}}}
                          @written*))
                   (is (= [["Bound ops to !room:example.org" "info"]]
                          @notifications*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest setup-command-delegates-to-setup-runner-with-ui-functions
  (async done
    (let [deps-seen* (atom nil)
          deps {:run-setup! (fn [setup-deps]
                              (reset! deps-seen* setup-deps)
                              (js/Promise.resolve {:matrix {:connected true}}))}
          ctx #js {:ui #js {:input (fn [_ _] (js/Promise.resolve "input"))
                            :editor (fn [_ _] (js/Promise.resolve "editor"))
                            :confirm (fn [_ _] (js/Promise.resolve true))
                            :notify (fn [_ _])
                            :setStatus (fn [_ _])}}]
      (-> (extension/handle-command! deps "setup" ctx)
          (.then (fn [_]
                   (is (fn? (:input! @deps-seen*)))
                   (is (fn? (:editor! @deps-seen*)))
                   (is (fn? (:confirm! @deps-seen*)))
                   (is (fn? (:notify! @deps-seen*)))
                   (is (fn? (:set-status! @deps-seen*)))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-can-run-diagnostics-through-compat-target
  (async done
    (let [deps {:relay-state* (atom {:client-id "client-1"
                                     :project-config {}
                                     :project {:id "project"}
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :pending-auto-replies* (atom [])})
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (js/Promise.resolve {:matrix {:connected true}}))
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:projectId project-id :slots []}))
                :list-rooms! (fn []
                               (js/Promise.resolve {:rooms []}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "matrix-relay:diagnostics"
                                                        :message "ignored"}
                                                ctx)
          (.then (fn [result]
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "pi-matrix-relay diagnostics"))
                   (is (= "!slot:example.org" (get-in result [:details :relay :roomId])))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-can-restart-through-compat-target
  (async done
    (let [calls* (atom [])
          relay-state* (atom {:client-id "client-1"
                              :project {:id "project"}
                              :slot "A"
                              :room-id "!old-slot:example.org"
                              :stream #js {:close (fn []
                                                    (swap! calls* conj [:close-old-stream]))}})
          deps {:relay-state* relay-state*
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (js/Promise.resolve {:matrix {:connected true}}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:clientId "client-2"
                                                         :heartbeatSeconds 30
                                                         :globalOperators []}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "B"
                                                      :roomId "!new-slot:example.org"
                                                      :roomName "project-B"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id _message _opts]
                                (swap! calls* conj [:send-message room-id])
                                (js/Promise.resolve {:eventId "$event:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [_client-id _reason]
                                      (js/Promise.resolve {}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeatSeconds 30}))
                :set-interval! (fn [_f _ms] :interval-2)
                :open-event-stream! (fn [_opts _client-id _on-event]
                                      #js {:close (fn [])})
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:projectId project-id :slots []}))
                :pi #js {}}
          ctx #js {:cwd "/work/project"
                   :ui #js {:setStatus (fn [_id _status])}}]
      (-> (extension/execute-send-matrix-message! deps {:target "matrix-relay:control"
                                                        :message "restart"}
                                                ctx)
          (.then (fn [result]
                   (is (some #{[:close-old-stream]} @calls*))
                   (is (some #{[:release-slot "client-1" "!old-slot:example.org" "A"]} @calls*))
                   (is (= "client-2" (:client-id @relay-state*)))
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "extension: running slot B project-B"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-reuses-bound-target-resolution
  (async done
    (let [sent* (atom nil)
          deps {:relay-state* (atom {:client-id "client-1"})
                :read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :send-message! (fn [room-id message opts]
                                (reset! sent* {:room-id room-id :message message :opts opts})
                                (js/Promise.resolve {:eventId "$event:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "ops"
                                                        :message "tool says hello"
                                                        :replyToEventId "$parent:example.org"}
                                                ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org"
                           :message "tool says hello"
                           :opts {:clientId "client-1"
                                  :replyToEventId "$parent:example.org"}}
                          @sent*))
                   (is (= {:content [{:type "text"
                                      :text "Sent Matrix message $event:example.org to !room:example.org"}]
                           :details {:roomId "!room:example.org"
                                     :eventId "$event:example.org"
                                     :target "ops"
                                     :replyToEventId "$parent:example.org"}}
                          result))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-accepts-raw-matrix-room-id
  (async done
    (let [sent* (atom nil)
          deps {:relay-state* (atom {:client-id "client-1"})
                :read-project-config! (fn [_cwd] {})
                :send-message! (fn [room-id message opts]
                                (reset! sent* {:room-id room-id :message message :opts opts})
                                (js/Promise.resolve {:eventId "$event:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "!slot:example.org"
                                                        :message "raw room hello"
                                                        :replyToEventId "$parent:example.org"}
                                                ctx)
          (.then (fn [result]
                   (is (= {:room-id "!slot:example.org"
                           :message "raw room hello"
                           :opts {:clientId "client-1"
                                  :replyToEventId "$parent:example.org"}}
                          @sent*))
                   (is (= "!slot:example.org" (get-in result [:details :roomId])))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-reaction-tool-reuses-bound-target-resolution
  (async done
    (let [sent* (atom nil)
          deps {:relay-state* (atom {:client-id "client-1"})
                :read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :send-reaction! (fn [room-id event-id key opts]
                                  (reset! sent* {:room-id room-id
                                                 :event-id event-id
                                                 :key key
                                                 :opts opts})
                                  (js/Promise.resolve {:eventId "$reaction:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-reaction! deps {:target "ops"
                                                         :eventId "$event:example.org"
                                                         :key "👍"}
                                                 ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org"
                           :event-id "$event:example.org"
                           :key "👍"
                           :opts {:clientId "client-1"}}
                          @sent*))
                   (is (= {:content [{:type "text"
                                      :text "Sent Matrix reaction 👍 to $event:example.org in !room:example.org"}]
                           :details {:roomId "!room:example.org"
                                     :eventId "$reaction:example.org"
                                     :reactsToEventId "$event:example.org"
                                     :target "ops"
                                     :key "👍"}}
                          result))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-reaction-tool-accepts-raw-matrix-room-id
  (async done
    (let [sent* (atom nil)
          deps {:relay-state* (atom {:client-id "client-1"})
                :read-project-config! (fn [_cwd] {})
                :send-reaction! (fn [room-id event-id key opts]
                                  (reset! sent* {:room-id room-id
                                                 :event-id event-id
                                                 :key key
                                                 :opts opts})
                                  (js/Promise.resolve {:eventId "$reaction:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-reaction! deps {:target "!slot:example.org"
                                                         :eventId "$event:example.org"
                                                         :key "👍"}
                                                 ctx)
          (.then (fn [result]
                   (is (= {:room-id "!slot:example.org"
                           :event-id "$event:example.org"
                           :key "👍"
                           :opts {:clientId "client-1"}}
                          @sent*))
                   (is (= "!slot:example.org" (get-in result [:details :roomId])))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest matrix-relay-diagnostics-tool-reports-extension-and-broker-state
  (async done
    (let [diagnostics* (atom {:events [{:at "2026-05-17T16:31:27.000Z"
                                        :type "start-error"
                                        :message "Route not found"}]})
          deps {:relay-state* (atom {:client-id "matrix-relay-/work/project"
                                     :project-config {:rooms {"ops" {:alias "ops"
                                                                     :roomId "!room:example.org"}}}
                                     :project {:id "project"
                                               :root "/work/project"
                                               :displayName "project"}
                                     :global-operators #{"@alice:example.org"}
                                     :bot-user-id "@bot:example.org"
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :pending-auto-replies* (atom [{:room-id "!slot:example.org"
                                                                    :event-id "$event:example.org"}])
                                     :heartbeat-id :interval-1
                                     :stream #js {:diagnostics (fn []
                                                                 #js {:connected true
                                                                      :eventCount 3})}})
                :diagnostics* diagnostics*
                :read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :health! (fn []
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:projectId project-id
                                                    :slots [{:slot "A"
                                                             :roomId "!slot:example.org"
                                                             :roomName "project-A"
                                                             :clientId "matrix-relay-/work/project"
                                                             :state "leased"}]}))
                :list-rooms! (fn []
                               (js/Promise.resolve {:rooms [{:roomId "!slot:example.org"
                                                             :name "project-A"}]}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-matrix-relay-diagnostics! deps {:includeBroker true
                                                             :includeRooms true}
                                                     ctx)
          (.then (fn [result]
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "extension: running slot A project-A"))
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "recent extension errors:"))
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "start-error: Route not found"))
                   (is (= [{:at "2026-05-17T16:31:27.000Z"
                            :type "start-error"
                            :message "Route not found"}]
                          (get-in result [:details :diagnostics :recentErrors])))
                   (is (= {:running true
                           :clientId "matrix-relay-/work/project"
                           :slot "A"
                           :roomId "!slot:example.org"
                           :roomName "project-A"
                           :heartbeatActive true
                           :streamActive true
                           :pendingAutoReplies 1}
                          (select-keys (get-in result [:details :relay])
                                       [:running :clientId :slot :roomId :roomName
                                        :heartbeatActive :streamActive :pendingAutoReplies])))
                   (is (= {:connected true
                           :eventCount 3}
                          (get-in result [:details :relay :streamDiagnostics])))
                   (is (= {:matrix {:connected true
                                     :userId "@bot:example.org"}}
                          (get-in result [:details :broker :health])))
                   (is (= [{:slot "A"
                            :roomId "!slot:example.org"
                            :roomName "project-A"
                            :clientId "matrix-relay-/work/project"
                            :state "leased"}]
                          (get-in result [:details :broker :slots :slots])))
                   (is (= [{:at "2026-05-17T16:31:27.000Z"
                            :type "start-error"
                            :message "Route not found"}]
                          (get-in result [:details :diagnostics :events])))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest matrix-relay-control-restarts-relay-from-agent-tool
  (async done
    (let [calls* (atom [])
          relay-state* (atom {:client-id "client-1"
                              :project {:id "project"}
                              :slot "A"
                              :room-id "!old-slot:example.org"
                              :stream #js {:close (fn []
                                                    (swap! calls* conj [:close-old-stream]))}})
          deps {:relay-state* relay-state*
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true}}))
                :register-client! (fn [request]
                                    (swap! calls* conj [:register (:clientInstanceId request)])
                                    (js/Promise.resolve {:clientId "client-2"
                                                         :heartbeatSeconds 30
                                                         :globalOperators []}))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve {:slot "B"
                                                      :roomId "!new-slot:example.org"
                                                      :roomName "project-B"}))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id (boolean (seq message)) opts])
                                (js/Promise.resolve {:eventId "$event:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [client-id reason]
                                      (swap! calls* conj [:unregister-client client-id reason])
                                      (js/Promise.resolve {}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeatSeconds 30}))
                :set-interval! (fn [_f _ms] :interval-2)
                :open-event-stream! (fn [_opts client-id _on-event]
                                      (swap! calls* conj [:open-event-stream client-id])
                                      #js {:close (fn [])})
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:projectId project-id :slots []}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:setStatus (fn [_id _status])}}
          pi #js {}]
      (-> (extension/execute-matrix-relay-control! deps {:action "restart"} pi ctx)
          (.then (fn [result]
                   (is (some #{[:close-old-stream]} @calls*))
                   (is (some #{[:release-slot "client-1" "!old-slot:example.org" "A"]} @calls*))
                   (is (some #{[:register "matrix-relay-/work/project"]} @calls*))
                   (is (= "client-2" (:client-id @relay-state*)))
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "extension: running slot B project-B"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-command-resolves-bound-target-and-posts-message
  (async done
    (let [sent* (atom nil)
          notifications* (atom [])
          deps {:read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :send-message! (fn [room-id message]
                                (reset! sent* {:room-id room-id :message message})
                                (js/Promise.resolve {:eventId "$event:example.org"}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "send ops hello from pi" ctx)
          (.then (fn [_]
                   (is (= {:room-id "!room:example.org" :message "hello from pi"}
                          @sent*))
                   (is (= [["Sent Matrix message $event:example.org" "info"]]
                          @notifications*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest session-start-registers-project-and-slot-subscriptions
  (async done
    (let [calls* (atom [])
          stream* (atom nil)
          intervals* (atom [])
          deps {:read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"
                                                        :mode "all"}}})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :register-client! (fn [request]
                                    (swap! calls* conj [:register request])
                                    (js/Promise.resolve {:clientId "client-1"
                                                         :eventStream "/v1/clients/client-1/events"
                                                         :heartbeatSeconds 30
                                                         :globalOperators ["@alice:example.org"]}))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve {:slot "A"
                                                      :roomId "!slot:example.org"
                                                      :roomName "project-A"}))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id message opts])
                                (js/Promise.resolve {:eventId "$start:example.org"}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeatSeconds 30}))
                :set-interval! (fn [f ms]
                                 (swap! intervals* conj {:f f :ms ms})
                                 :interval-1)
                :open-event-stream! (fn [opts client-id on-event]
                                      (swap! calls* conj [:open-event-stream client-id (fn? (:on-error opts))])
                                      (reset! stream* {:client-id client-id
                                                       :on-event on-event
                                                       :closed? (atom false)})
                                      #js {:close (fn []
                                                    (reset! (:closed? @stream*) true))})}
          notifications* (atom [])
          statuses* (atom [])
          pi #js {:sendUserMessage (fn [_message])}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))
                            :setStatus (fn [id status]
                                         (swap! statuses* conj [id status]))}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [relay-state]
                   (is (= [[:health]
                           [:register {:clientInstanceId "matrix-relay-/work/project"
                                       :protocolVersion 1
                                       :project {:root "/work/project"
                                                 :id "project"}
                                       :subscriptions {:rooms ["!room:example.org"]}}]
                           [:acquire-slot "client-1" {:id "project" :displayName "project"} ["@alice:example.org"]]
                           [:update-subscriptions "client-1" ["!room:example.org" "!slot:example.org"]]
                           [:send-message "!slot:example.org" (:last-start-banner relay-state) {:clientId "client-1"}]
                           [:open-event-stream "client-1" true]]
                          @calls*))
                   (is (= [{:ms 30000 :has-fn? true}]
                          (mapv (fn [{:keys [f ms]}] {:ms ms :has-fn? (fn? f)}) @intervals*)))
                   (is (= {:slot "A"
                           :room-id "!slot:example.org"
                           :room-name "project-A"
                           :heartbeat-id :interval-1}
                          (select-keys relay-state [:slot :room-id :room-name :heartbeat-id])))
                   (is (= [["pi-matrix-relay" "matrix: slot A project-A; rooms: ops"]]
                          @statuses*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest session-start-event-handler-reports-stream-active-in-remote-status
  (async done
    (let [sent* (atom [])
          stream* (atom nil)
          deps {:read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:clientId "client-1"
                                                         :heartbeatSeconds 30
                                                         :globalOperators ["@alice:example.org"]}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "A"
                                                      :roomId "!slot:example.org"
                                                      :roomName "project-A"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! sent* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:eventId "$sent:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeatSeconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :open-event-stream! (fn [_opts _client-id on-event]
                                      (reset! stream* {:on-event on-event})
                                      #js {:close (fn [])})}
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"
                   :model #js {:provider "anthropic"
                               :id "claude-sonnet"}
                   :ui #js {:setStatus (fn [_id _status])}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [_relay-state]
                   ((:on-event @stream*)
                    {:type "matrix.message"
                     :room {:roomId "!slot:example.org"}
                     :event {:eventId "$status:example.org"
                             :sender "@alice:example.org"
                             :senderIsBot false
                             :timestamp "2026-05-16T12:34:56Z"
                             :text "/status"}})
                   (is (str/includes? (:message (last @sent*)) "stream: active"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest session-start-event-handler-injects-debug-context-on-event-error
  (async done
    (let [sent* (atom [])
          stream* (atom nil)
          diagnostics* (atom {})
          deps {:diagnostics* diagnostics*
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:clientId "client-1"
                                                         :heartbeatSeconds 30
                                                         :globalOperators ["@alice:example.org"]}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "A"
                                                      :roomId "!slot:example.org"
                                                      :roomName "project-A"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [_room-id _message _opts]
                                (js/Promise.resolve {:eventId "$sent:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeatSeconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :open-event-stream! (fn [_opts _client-id on-event]
                                      (reset! stream* {:on-event on-event})
                                      #js {:close (fn [])})}
          pi #js {:sendUserMessage (fn [message options]
                                     (swap! sent* conj {:message message
                                                        :options (some-> options (js->clj :keywordize-keys true))}))}
          ctx #js {:cwd "/work/project"
                   :isIdle (fn []
                             (throw (js/Error. "idle check failed")))
                   :ui #js {:setStatus (fn [_id _status])}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [_relay-state]
                   ((:on-event @stream*)
                    {:type "matrix.message"
                     :room {:roomId "!slot:example.org"}
                     :event {:eventId "$event:example.org"
                             :sender "@alice:example.org"
                             :senderIsBot false
                             :timestamp "2026-05-16T12:34:56Z"
                             :text "ordinary slot prompt"}})
                   (is (= 1 (count @sent*)))
                   (is (str/includes? (:message (first @sent*)) "pi-matrix-relay extension error"))
                   (is (str/includes? (:message (first @sent*)) "source: broker-event"))
                   (is (str/includes? (:message (first @sent*)) "eventId: $event:example.org"))
                   (is (str/includes? (:message (first @sent*)) "error: idle check failed"))
                   (is (= {:deliverAs "followUp"} (:options (first @sent*))))
                   (is (some #(= "broker-event-error" (:type %))
                             (:events @diagnostics*)))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest session-start-acquires-slot-without-project-rooms
  (async done
    (let [calls* (atom [])
          deps {:read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :register-client! (fn [request]
                                    (swap! calls* conj [:register request])
                                    (js/Promise.resolve {:clientId "client-1"
                                                         :heartbeatSeconds 30
                                                         :globalOperators ["@alice:example.org"]}))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve {:slot "A"
                                                      :roomId "!slot:example.org"
                                                      :roomName "project-A"}))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id (boolean (seq message)) opts])
                                (js/Promise.resolve {:eventId "$start:example.org"}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeatSeconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :open-event-stream! (fn [opts client-id _on-event]
                                      (swap! calls* conj [:open-event-stream client-id (fn? (:on-error opts))])
                                      #js {:close (fn [])})}
          statuses* (atom [])
          pi #js {:sendUserMessage (fn [_message])}
          ctx #js {:cwd "/work/project"
                   :ui #js {:setStatus (fn [id status]
                                         (swap! statuses* conj [id status]))}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [_relay-state]
                   (is (= [[:health]
                           [:register {:clientInstanceId "matrix-relay-/work/project"
                                       :protocolVersion 1
                                       :project {:root "/work/project"
                                                 :id "project"}
                                       :subscriptions {:rooms []}}]
                           [:acquire-slot "client-1" {:id "project" :displayName "project"} ["@alice:example.org"]]
                           [:update-subscriptions "client-1" ["!slot:example.org"]]
                           [:send-message "!slot:example.org" true {:clientId "client-1"}]
                           [:open-event-stream "client-1" true]]
                          @calls*))
                   (is (= [["pi-matrix-relay" "matrix: slot A project-A"]]
                          @statuses*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest stop-relay-sends-tombstone-and-releases-slot
  (async done
    (let [calls* (atom [])
          statuses* (atom [])
          deps {:clear-interval! (fn [heartbeat-id]
                                   (swap! calls* conj [:clear-interval heartbeat-id]))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id (boolean (seq message)) opts])
                                (js/Promise.resolve {:eventId "$end:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [client-id reason]
                                      (swap! calls* conj [:unregister-client client-id reason])
                                      (js/Promise.resolve {}))}
          relay-state {:client-id "client-1"
                       :project {:id "project"}
                       :slot "A"
                       :room-id "!slot:example.org"
                       :room-name "project-A"
                       :heartbeat-id :interval-1
                       :stream #js {:close (fn []
                                             (swap! calls* conj [:close-stream]))}}
          ctx #js {:ui #js {:setStatus (fn [id status]
                                         (swap! statuses* conj [id (undefined? status)]))}}]
      (-> (js/Promise.resolve (extension/stop-relay! deps ctx relay-state))
          (.then (fn [_]
                   (is (= [[:close-stream]
                           [:clear-interval :interval-1]
                           [:send-message "!slot:example.org" true {:clientId "client-1"}]
                           [:release-slot "client-1" "!slot:example.org" "A"]
                           [:unregister-client "client-1" "shutdown"]]
                          @calls*))
                   (is (= [["pi-matrix-relay" true]] @statuses*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest stop-relay-releases-slot-even-when-tombstone-send-fails
  (async done
    (let [calls* (atom [])
          deps {:clear-interval! (fn [heartbeat-id]
                                   (swap! calls* conj [:clear-interval heartbeat-id]))
                :send-message! (fn [_room-id _message _opts]
                                (swap! calls* conj [:send-message])
                                (js/Promise.reject (js/Error. "send failed")))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [client-id reason]
                                      (swap! calls* conj [:unregister-client client-id reason])
                                      (js/Promise.resolve {}))}
          relay-state {:client-id "client-1"
                       :project {:id "project"}
                       :slot "A"
                       :room-id "!slot:example.org"
                       :heartbeat-id :interval-1
                       :stream #js {:close (fn []
                                             (swap! calls* conj [:close-stream]))}}
          ctx #js {:ui #js {:setStatus (fn [_id _status])}}]
      (-> (js/Promise.resolve (extension/stop-relay! deps ctx relay-state))
          (.then (fn [_]
                   (is (= [[:close-stream]
                           [:clear-interval :interval-1]
                           [:send-message]
                           [:release-slot "client-1" "!slot:example.org" "A"]
                           [:unregister-client "client-1" "shutdown"]]
                          @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest authorized-slot-room-message-is-injected-with-all-mode
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$slot-event:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "no bot mention needed in slot rooms"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "Matrix project-A from @alice:example.org at 12:34:56Z"))
    (is (str/includes? (:message (first @sent*)) "no bot mention needed in slot rooms"))))

(deftest slot-room-message-records-auto-reply-target-after-delivery
  (let [sent* (atom [])
        pending* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :pending-auto-replies* pending*}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$slot-event:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "please respond"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (= [{:room-id "!slot:example.org"
             :event-id "$slot-event:example.org"}]
           @pending*))))

(deftest project-room-message-does-not-record-auto-reply-target
  (let [sent* (atom [])
        pending* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {:rooms {"ops" {:alias "ops"
                                                      :roomId "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :pending-auto-replies* pending*}
        event {:type "matrix.message"
               :room {:roomId "!room:example.org"}
               :event {:eventId "$project-event:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "project prompt"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (= [] @pending*))))

(deftest rejected-slot-room-message-does-not-record-auto-reply-target
  (let [sent* (atom [])
        pending* (atom [])
        notifications* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] false)
                 :ui #js {:notify (fn [message level]
                                    (swap! notifications* conj [message level]))}}
        relay-state {:project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :pending-auto-replies* pending*}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$slot-event:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "please respond"}}]
    (with-redefs [config/default-busy-behavior "reject"]
      (extension/handle-broker-event! {} pi ctx relay-state event))
    (is (= [] @sent*))
    (is (= [] @pending*))))

(deftest matrix-status-command-sends-room-ack-without-injecting-prompt
  (let [sent* (atom [])
        acks* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        branch #js [#js {:type "message"
                         :message #js {:role "assistant"
                                       :usage #js {:input 360000
                                                   :output 14000
                                                   :cost #js {:total 4.591}}}}]
        session-manager (let [sm (js-obj)]
                          (aset sm "branch" branch)
                          (aset sm "getBranch" (fn []
                                                  (this-as this
                                                    (.-branch ^js this))))
                          sm)
        ctx #js {:cwd "/work/project"
                 :model #js {:provider "openai-codex"
                             :id "gpt-5.5"
                             :contextWindow 272000}
                 :getContextUsage (fn []
                                    #js {:tokens 123456
                                         :contextWindow 272000
                                         :percent 45.4})
                 :sessionManager session-manager
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :heartbeat-id :heartbeat-1
                     :stream #js {}}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$status:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "//status"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= [] @sent*))
    (is (= 1 (count @acks*)))
    (is (= {:clientId "client-1"
            :replyToEventId "$status:example.org"}
           (:opts (first @acks*))))
    (is (str/includes? (:message (first @acks*)) "pi-matrix-relay status"))
    (is (str/includes? (:message (first @acks*)) "slot: A project-A"))
    (is (str/includes? (:message (first @acks*)) "model: openai-codex/gpt-5.5"))
    (is (str/includes? (:message (first @acks*)) "context: 123456 tokens (45%/272k)"))
    (is (str/includes? (:message (first @acks*)) "usage: ↑360.0k ↓14.0k $4.591"))))

(deftest matrix-status-command-still-acks-when-context-usage-is-unavailable
  (let [sent* (atom [])
        acks* (atom [])
        thrown* (atom nil)
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        branch #js [#js {:type "message"
                         :message #js {:role "assistant"
                                       :usage #js {:input 10
                                                   :output 5
                                                   :cost #js {:total 0.01}}}}]
        session-manager (let [sm (js-obj)]
                          (aset sm "branch" branch)
                          (aset sm "getBranch" (fn []
                                                  (this-as this
                                                    (.-branch ^js this))))
                          sm)
        ctx #js {:cwd "/work/project"
                 :model #js {:provider "anthropic"
                             :id "claude-sonnet"
                             :contextWindow 200000}
                 :getContextUsage (fn []
                                    (throw (js/Error. "Cannot read properties of null (reading 'leafId')")))
                 :sessionManager session-manager
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :heartbeat-id :heartbeat-1
                     :stream #js {}}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$status:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/status"}}]
    (try
      (extension/handle-broker-event! deps pi ctx relay-state event)
      (catch js/Error err
        (reset! thrown* err)))
    (is (nil? @thrown*) (some-> @thrown* .-message))
    (is (= [] @sent*))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "pi-matrix-relay status"))
    (is (str/includes? (:message (first @acks*)) "model: anthropic/claude-sonnet"))
    (is (str/includes? (:message (first @acks*)) "context: ?"))
    (is (str/includes? (:message (first @acks*)) "usage: ↑10 ↓5 $0.010"))))

(deftest matrix-status-command-still-acks-when-branch-usage-is-unavailable
  (let [sent* (atom [])
        acks* (atom [])
        thrown* (atom nil)
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :model #js {:provider "anthropic"
                             :id "claude-sonnet"
                             :contextWindow 200000}
                 :getContextUsage (fn []
                                    #js {:tokens 50000
                                         :contextWindow 200000
                                         :percent 25})
                 :sessionManager #js {:getBranch (fn []
                                                   (throw (js/Error. "branch is not available")))}
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :heartbeat-id :heartbeat-1
                     :stream #js {}}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$status:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/status"}}]
    (try
      (extension/handle-broker-event! deps pi ctx relay-state event)
      (catch js/Error err
        (reset! thrown* err)))
    (is (nil? @thrown*) (some-> @thrown* .-message))
    (is (= [] @sent*))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "context: 50000 tokens (25%/200k)"))
    (is (str/includes? (:message (first @acks*)) "usage: ↑0 ↓0 $0.000"))))

(deftest matrix-remote-command-error-sends-room-ack-and-debug-context
  (let [acks* (atom [])
        sent* (atom [])
        thrown* (atom nil)
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :abort (fn []
                          (throw (js/Error. "abort failed")))}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:diagnostics* (atom {})
              :send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$abort:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/abort"}}]
    (try
      (extension/handle-broker-event! deps pi ctx relay-state event)
      (catch js/Error err
        (reset! thrown* err)))
    (is (nil? @thrown*) (some-> @thrown* .-message))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "Remote command /abort failed: abort failed"))
    (is (= {:clientId "client-1"
            :replyToEventId "$abort:example.org"}
           (:opts (first @acks*))))
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "pi-matrix-relay extension error"))
    (is (str/includes? (:message (first @sent*)) "source: remote-command"))
    (is (str/includes? (:message (first @sent*)) "command: /abort"))
    (is (str/includes? (:message (first @sent*)) "eventId: $abort:example.org"))
    (is (str/includes? (:message (first @sent*)) "error: abort failed"))
    (is (str/includes? (:message (first @sent*)) "matrix_relay_diagnostics"))
    (is (= {:deliverAs "followUp"} (:options (first @sent*))))
    (is (some? (:stack (first (:events @(:diagnostics* deps))))))))

(deftest matrix-remote-command-error-debug-context-can-be-disabled
  (let [acks* (atom [])
        sent* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :abort (fn []
                          (throw (js/Error. "abort failed")))}
        relay-state {:client-id "client-1"
                     :project-config {:debug {:enabled false}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:diagnostics* (atom {})
              :send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$abort:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/abort"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "Remote command /abort failed: abort failed"))
    (is (= [] @sent*))))

(deftest matrix-steer-command-injects-steering-prompt-and-sends-ack
  (let [sent* (atom [])
        acks* (atom [])
        pending* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] false)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :pending-auto-replies* pending*}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$steer:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/steer focus on the failing test"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "focus on the failing test"))
    (is (= {:deliverAs "steer"} (:options (first @sent*))))
    (is (= [{:room-id "!slot:example.org"
             :event-id "$steer:example.org"}]
           @pending*))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "Steering message sent"))))

(deftest matrix-follow-up-command-injects-follow-up-prompt-and-sends-ack
  (let [sent* (atom [])
        acks* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] false)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :pending-auto-replies* (atom [])}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$follow-up:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/follow-up run the next check"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "run the next check"))
    (is (= {:deliverAs "followUp"} (:options (first @sent*))))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "Follow-up message queued"))))

(deftest bare-steer-command-changes-room-default-and-acks
  (let [sent* (atom [])
        acks* (atom [])
        behaviors* (atom {})
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] false)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :room-behaviors* behaviors*
                     :pending-auto-replies* (atom [])}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        steer-event {:type "matrix.message"
                     :room {:roomId "!slot:example.org"}
                     :event {:eventId "$set-steer:example.org"
                             :sender "@alice:example.org"
                             :timestamp "2026-05-16T12:34:56Z"
                             :text "/steer"}}
        prompt-event {:type "matrix.message"
                      :room {:roomId "!slot:example.org"}
                      :event {:eventId "$next:example.org"
                              :sender "@alice:example.org"
                              :timestamp "2026-05-16T12:34:57Z"
                              :text "ordinary slot prompt"}}]
    (extension/handle-broker-event! deps pi ctx relay-state steer-event)
    (is (= {"!slot:example.org" "steer"} @behaviors*))
    (is (= [] @sent*))
    (is (str/includes? (:message (first @acks*)) "Default Matrix message behavior for this room is now steer"))
    (extension/handle-broker-event! deps pi ctx relay-state prompt-event)
    (is (= 1 (count @sent*)))
    (is (= {:deliverAs "steer"} (:options (first @sent*))))))

(deftest bare-follow-up-command-changes-room-default-and-acks
  (let [sent* (atom [])
        acks* (atom [])
        behaviors* (atom {"!slot:example.org" "steer"})
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] false)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :room-behaviors* behaviors*
                     :pending-auto-replies* (atom [])}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        follow-event {:type "matrix.message"
                      :room {:roomId "!slot:example.org"}
                      :event {:eventId "$set-follow-up:example.org"
                              :sender "@alice:example.org"
                              :timestamp "2026-05-16T12:34:56Z"
                              :text "/follow-up"}}
        prompt-event {:type "matrix.message"
                      :room {:roomId "!slot:example.org"}
                      :event {:eventId "$next:example.org"
                              :sender "@alice:example.org"
                              :timestamp "2026-05-16T12:34:57Z"
                              :text "ordinary slot prompt"}}]
    (extension/handle-broker-event! deps pi ctx relay-state follow-event)
    (is (= {"!slot:example.org" "follow-up"} @behaviors*))
    (is (= [] @sent*))
    (is (str/includes? (:message (first @acks*)) "Default Matrix message behavior for this room is now follow-up"))
    (extension/handle-broker-event! deps pi ctx relay-state prompt-event)
    (is (= 1 (count @sent*)))
    (is (= {:deliverAs "followUp"} (:options (first @sent*))))))

(deftest matrix-abort-command-aborts-current-turn-and-sends-ack
  (let [aborted? (atom false)
        acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        ctx #js {:cwd "/work/project"
                 :abort (fn []
                          (reset! aborted? true))}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$abort:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/abort"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (true? @aborted?))
    (is (str/includes? (:message (first @acks*)) "Abort requested"))
    (is (= {:clientId "client-1"
            :replyToEventId "$abort:example.org"}
           (:opts (first @acks*))))))

(deftest matrix-compact-command-starts-compaction-and-reports-completion
  (let [compact-options* (atom nil)
        acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        entries #js [#js {:type "message"}
                     #js {:type "message"}]
        session-manager (let [sm (js-obj)]
                          (aset sm "entries" entries)
                          (aset sm "getEntries" (fn []
                                                   (this-as this
                                                     (.-entries ^js this))))
                          sm)
        ctx #js {:cwd "/work/project"
                 :sessionManager session-manager
                 :compact (fn [options]
                            (reset! compact-options* options))}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$compact:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/compact focus on matrix relay changes"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (some? @compact-options*))
    (is (= "focus on matrix relay changes" (.-customInstructions ^js @compact-options*)))
    (is (str/includes? (:message (first @acks*)) "Compaction started"))
    ((.-onComplete ^js @compact-options*) #js {:tokensBefore 12345})
    (is (str/includes? (:message (last @acks*)) "Compaction completed"))
    (is (str/includes? (:message (last @acks*)) "12345"))))

(deftest matrix-compact-command-rejects-when-there-is-nothing-to-compact
  (let [compact-called? (atom false)
        acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        entries #js [#js {:type "message"}]
        session-manager (let [sm (js-obj)]
                          (aset sm "entries" entries)
                          (aset sm "getEntries" (fn []
                                                   (this-as this
                                                     (.-entries ^js this))))
                          sm)
        ctx #js {:cwd "/work/project"
                 :sessionManager session-manager
                 :compact (fn [_options]
                            (reset! compact-called? true))}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$compact:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/compact"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (false? @compact-called?))
    (is (str/includes? (:message (first @acks*)) "Nothing to compact"))))

(deftest matrix-new-command-starts-new-session-when-command-context-supports-it
  (async done
    (let [new-session-called? (atom false)
          acks* (atom [])
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"
                   :newSession (fn []
                                 (reset! new-session-called? true)
                                 (js/Promise.resolve #js {:cancelled false}))}
          relay-state {:client-id "client-1"
                       :project-config {}
                       :global-operators #{"@alice:example.org"}
                       :bot-user-id "@bot:example.org"
                       :slot "A"
                       :room-id "!slot:example.org"
                       :room-name "project-A"}
          deps {:send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:eventId "$ack:example.org"}))}
          event {:type "matrix.message"
                 :room {:roomId "!slot:example.org"}
                 :event {:eventId "$new:example.org"
                         :sender "@alice:example.org"
                         :timestamp "2026-05-16T12:34:56Z"
                         :text "/new"}}]
      (extension/handle-broker-event! deps pi ctx relay-state event)
      (js/setTimeout
       (fn []
         (is (true? @new-session-called?))
         (is (str/includes? (:message (first @acks*)) "New session started"))
         (done))
       0))))

(deftest matrix-new-command-reports-when-new-session-is-unavailable
  (let [acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        ctx #js {:cwd "/work/project"}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:eventId "$ack:example.org"}))}
        event {:type "matrix.message"
               :room {:roomId "!slot:example.org"}
               :event {:eventId "$new:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "/new"}}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (str/includes? (:message (first @acks*)) "New session is not available"))))

(deftest agent-end-sends-automatic-reply-for-pending-slot-prompt
  (async done
    (let [sent* (atom [])
          pending* (atom [{:room-id "!slot:example.org"
                           :event-id "$slot-event:example.org"}])
          deps {:send-message! (fn [room-id message opts]
                                (swap! sent* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:eventId "$reply:example.org"}))}
          relay-state {:client-id "client-1"
                       :pending-auto-replies* pending*}
          event {:messages [{:role "user" :content "prompt"}
                            {:role "assistant"
                             :stopReason "stop"
                             :content [{:type "text" :text "Final answer"}]}]}]
      (-> (extension/handle-agent-end! deps relay-state event)
          (.then (fn [_]
                   (is (= [{:room-id "!slot:example.org"
                            :message "Final answer"
                            :opts {:clientId "client-1"
                                   :replyToEventId "$slot-event:example.org"}}]
                          @sent*))
                   (is (= [] @pending*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest agent-end-clears-pending-slot-reply-when-send-fails
  (async done
    (let [sent* (atom [])
          pending* (atom [{:room-id "!slot:example.org"
                           :event-id "$slot-event:example.org"}])
          deps {:send-message! (fn [room-id message opts]
                                (swap! sent* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.reject (js/Error. "send timed out")))}
          relay-state {:client-id "client-1"
                       :pending-auto-replies* pending*}
          event {:messages [{:role "assistant"
                             :stopReason "stop"
                             :content [{:type "text" :text "Final answer"}]}]}]
      (-> (extension/handle-agent-end! deps relay-state event)
          (.then (fn [_]
                   (is (= 1 (count @sent*)))
                   (is (= [] @pending*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest agent-end-does-not-send-automatic-reply-without-pending-slot-prompt
  (async done
    (let [sent* (atom [])
          deps {:send-message! (fn [room-id message opts]
                                (swap! sent* conj [room-id message opts])
                                (js/Promise.resolve {}))}
          relay-state {:client-id "client-1"
                       :pending-auto-replies* (atom [])}
          event {:messages [{:role "assistant"
                             :stopReason "stop"
                             :content [{:type "text" :text "Project answer"}]}]}]
      (-> (extension/handle-agent-end! deps relay-state event)
          (.then (fn [_]
                   (is (= [] @sent*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest slot-room-message-from-bot-or-unauthorized-sender-is-ignored
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}]
    (doseq [event [{:type "matrix.message"
                    :room {:roomId "!slot:example.org"}
                    :event {:eventId "$bot:example.org"
                            :sender "@bot:example.org"
                            :senderIsBot true
                            :timestamp "2026-05-16T12:34:56Z"
                            :text "bot echo"}}
                   {:type "matrix.message"
                    :room {:roomId "!slot:example.org"}
                    :event {:eventId "$mallory:example.org"
                            :sender "@mallory:example.org"
                            :timestamp "2026-05-16T12:34:56Z"
                            :text "unauthorized"}}]]
      (extension/handle-broker-event! {} pi ctx relay-state event))
    (is (= [] @sent*))))

(deftest project-room-mentions-mode-still-requires-a-bot-mention
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {:rooms {"ops" {:alias "ops"
                                                      :roomId "!room:example.org"
                                                      :mode "mentions"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}]
    (extension/handle-broker-event!
     {}
     pi
     ctx
     relay-state
     {:type "matrix.message"
      :room {:roomId "!room:example.org"}
      :event {:eventId "$ignored:example.org"
              :sender "@alice:example.org"
              :timestamp "2026-05-16T12:34:56Z"
              :text "ordinary project chatter"}})
    (extension/handle-broker-event!
     {}
     pi
     ctx
     relay-state
     {:type "matrix.message"
      :room {:roomId "!room:example.org"}
      :event {:eventId "$mentioned:example.org"
              :sender "@alice:example.org"
              :timestamp "2026-05-16T12:34:57Z"
              :text "@bot please inspect this"}})
    (is (= 1 (count @sent*)))
    (is (str/includes? (first @sent*) "@bot please inspect this"))))

(deftest authorized-matrix-message-from-bound-room-is-injected-as-user-message
  (let [sent* (atom [])
        notifications* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)
                 :ui #js {:notify (fn [message level]
                                    (swap! notifications* conj [message level]))}}
        relay-state {:project-config {:rooms {"ops" {:alias "ops"
                                                      :roomId "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}
        event {:type "matrix.message"
               :room {:roomId "!room:example.org"}
               :event {:eventId "$event:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "please check status"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "Matrix ops from @alice:example.org at 12:34:56Z"))
    (is (str/includes? (:message (first @sent*)) "please check status"))
    (is (not (str/includes? (:message (first @sent*)) "roomId: !room:example.org")))
    (is (str/includes? (:message (first @sent*)) "eventId: $event:example.org"))
    (is (nil? (:options (first @sent*))))
    (is (= [] @notifications*))))

(deftest matrix-reaction-from-bound-room-is-injected-as-user-message
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {:rooms {"ops" {:alias "ops"
                                                      :roomId "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}
        event {:type "matrix.reaction"
               :room {:roomId "!room:example.org"}
               :event {:eventId "$reaction:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :reactsToEventId "$event:example.org"
                       :key "👍"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "Matrix reaction in ops from @alice:example.org at 12:34:56Z"))
    (is (str/includes? (:message (first @sent*)) "reacted 👍 to event $event:example.org"))
    (is (not (str/includes? (:message (first @sent*)) "roomId: !room:example.org")))))

(deftest matrix-message-includes-room-id-when-room-label-is-ambiguous
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {:rooms [{:name "Shared"
                                               :roomId "!first:example.org"
                                               :mode "all"}
                                              {:name "Shared"
                                               :roomId "!second:example.org"
                                               :mode "all"}]}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}
        event {:type "matrix.message"
               :room {:roomId "!first:example.org"}
               :event {:eventId "$event:example.org"
                       :sender "@alice:example.org"
                       :timestamp "2026-05-16T12:34:56+02:00"
                       :text "hello from ambiguous room"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (first @sent*) "Matrix Shared from @alice:example.org at 12:34:56+02:00"))
    (is (str/includes? (first @sent*) "roomId: !first:example.org"))))

(deftest unauthorized-matrix-message-is-ignored
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {:rooms {"ops" {:alias "ops"
                                                      :roomId "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}}
        event {:type "matrix.message"
               :room {:roomId "!room:example.org"}
               :event {:eventId "$event:example.org"
                       :sender "@mallory:example.org"
                       :timestamp "2026-05-16T12:34:56Z"
                       :text "please check status"}}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= [] @sent*))))
