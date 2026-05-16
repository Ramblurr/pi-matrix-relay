(ns pi-matrix-relay.extension-test
  (:require [cljs.test :refer [async deftest is testing]]
            [clojure.string :as str]
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
          deps {:read-project-config! (fn [_cwd]
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
                           :opts {:replyToEventId "$parent:example.org"}}
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
          deps {:read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :send-reaction! (fn [room-id event-id key]
                                  (reset! sent* {:room-id room-id
                                                 :event-id event-id
                                                 :key key})
                                  (js/Promise.resolve {:eventId "$reaction:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-reaction! deps {:target "ops"
                                                         :eventId "$event:example.org"
                                                         :key "👍"}
                                                 ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org"
                           :event-id "$event:example.org"
                           :key "👍"}
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

(deftest session-start-registers-project-room-subscriptions-and-opens-event-stream
  (async done
    (let [calls* (atom [])
          stream* (atom nil)
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
                                                         :globalOperators ["@alice:example.org"]}))
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
          (.then (fn [_]
                   (is (= [:health] (first @calls*)))
                   (is (= [:register {:clientInstanceId "matrix-relay-/work/project"
                                      :protocolVersion 1
                                      :project {:root "/work/project"
                                                :id "project"}
                                      :subscriptions {:rooms ["!room:example.org"]}}]
                          (second @calls*)))
                   (is (= [:open-event-stream "client-1"] (nth @calls* 2)))
                   (is (= [["pi-matrix-relay" "matrix: listening to ops"]]
                          @statuses*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

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
    (is (str/includes? (:message (first @sent*)) "Matrix ops from @alice:example.org at 12:34"))
    (is (str/includes? (:message (first @sent*)) "please check status"))
    (is (str/includes? (:message (first @sent*)) "roomId: !room:example.org"))
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
    (is (str/includes? (:message (first @sent*)) "Matrix reaction in ops from @alice:example.org at 12:34"))
    (is (str/includes? (:message (first @sent*)) "reacted 👍 to event $event:example.org"))))

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
