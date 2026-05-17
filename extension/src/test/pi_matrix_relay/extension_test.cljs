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
        pi #js {:registerCommand (fn [name opts]
                                   (swap! commands* assoc name opts))
                :registerTool (fn [tool]
                                (swap! tools* assoc (.-name tool) tool))}]
    (extension/init pi)
    (is (= #{"matrix-relay" "mr"}
           (set (keys @commands*))))
    (is (= #{"send_matrix_message" "send_matrix_reaction"}
           (set (keys @tools*))))
    (is (fn? (.-execute ^js (get @tools* "send_matrix_message"))))
    (is (fn? (.-execute ^js (get @tools* "send_matrix_reaction"))))))

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
                :open-event-stream! (fn [client-id on-event]
                                      (swap! calls* conj [:open-event-stream client-id])
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
                           [:open-event-stream "client-1"]]
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
                :open-event-stream! (fn [client-id _on-event]
                                      (swap! calls* conj [:open-event-stream client-id])
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
                           [:open-event-stream "client-1"]]
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
