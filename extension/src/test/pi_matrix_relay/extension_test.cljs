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
    (let [send-tool ^js (get @tools* "send_matrix_message")
          params (js->clj (.-parameters send-tool) :keywordize-keys true)
          formatted-desc (str (get-in params [:properties :formattedBody :description]))]
      (is (fn? (.-execute send-tool)))
      (is (= {:type "string"}
             (select-keys (get-in params [:properties :formattedBody]) [:type])))
      (is (every? #(str/includes? formatted-desc %)
                  ["del" "h1" "h6" "blockquote" "table" "caption" "pre" "img" "details" "summary"
                   "data-mx-bg-color" "data-mx-color" "data-mx-spoiler" "data-mx-maths"
                   "https" "mailto" "magnet" "mxc://" "language-" "100"])))
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
                                 (js/Promise.resolve {:room/id "!room:example.org"
                                                      :room/canonical-alias room
                                                      :room/name "Pi Room"}))
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
                                                   :room/id "!room:example.org"
                                                   :room/canonical-alias "#pi:example.org"
                                                   :room/name "Pi Room"
                                                   :mode "mentions"}}}}
                          @written*))
                   (is (= [["Bound ops to !room:example.org with mode mentions" "info"]]
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
                              (js/Promise.resolve {:matrix/connected? true}))}
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
                                     :project {:project/id "project"}
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :pending-auto-replies* (atom [])})
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (js/Promise.resolve {:matrix/connected? true}))
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:project/id project-id :slots []}))
                :list-rooms! (fn []
                               (js/Promise.resolve {:rooms []}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "matrix-relay:diagnostics"
                                                        :message "ignored"}
                                                ctx)
          (.then (fn [result]
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "pi-matrix-relay diagnostics"))
                   (is (= "!slot:example.org" (get-in result [:details :relay :room/id])))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-can-restart-through-compat-target
  (async done
    (let [calls* (atom [])
          relay-state* (atom {:client-id "client-1"
                              :project {:project/id "project"}
                              :slot "A"
                              :room-id "!old-slot:example.org"
                              :stream #js {:close (fn []
                                                    (swap! calls* conj [:close-old-stream]))}})
          deps {:relay-state* relay-state*
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (js/Promise.resolve {:matrix/connected? true}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:client/id "client-2"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators []}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "B"
                                                      :room/id "!new-slot:example.org"
                                                      :room/name "project-B"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id _message _opts]
                                (swap! calls* conj [:send-message room-id])
                                (js/Promise.resolve {:event/id "$event:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [_client-id _reason]
                                      (js/Promise.resolve {}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms] :interval-2)
                :open-event-stream! (fn [_opts _client-id _on-event]
                                      #js {:close (fn [])})
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:project/id project-id :slots []}))
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
                                                        :room/id "!room:example.org"}}})
                :send-message! (fn [room-id message opts]
                                (reset! sent* {:room-id room-id :message message :opts opts})
                                (js/Promise.resolve {:event/id "$event:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "ops"
                                                        :message "tool says hello"
                                                        :reply-to/event-id "$parent:example.org"}
                                                ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org"
                           :message "tool says hello"
                           :opts {:client/id "client-1"
                                  :reply-to/event-id "$parent:example.org"}}
                          @sent*))
                   (is (= {:content [{:type "text"
                                      :text "Sent Matrix message $event:example.org to !room:example.org"}]
                           :details {:room/id "!room:example.org"
                                     :event/id "$event:example.org"
                                     :target "ops"
                                     :reply-to/event-id "$parent:example.org"}}
                          result))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-passes-formatted-body-to-broker
  (async done
    (let [sent* (atom nil)
          deps {:relay-state* (atom {:client-id "client-1"})
                :read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :room/id "!room:example.org"}}})
                :send-message! (fn [room-id message opts]
                                (reset! sent* {:room-id room-id :message message :opts opts})
                                (js/Promise.resolve {:event/id "$event:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "ops"
                                                        :message "plain fallback"
                                                        :formattedBody "<strong>formatted</strong>"
                                                        :replyToEventId "$parent:example.org"}
                                                ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org"
                           :message "plain fallback"
                           :opts {:client/id "client-1"
                                  :reply-to/event-id "$parent:example.org"
                                  :formatted-body "<strong>formatted</strong>"}}
                          @sent*))
                   (is (= {:room/id "!room:example.org"
                           :event/id "$event:example.org"
                           :target "ops"
                           :reply-to/event-id "$parent:example.org"}
                          (:details result)))
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
                                (js/Promise.resolve {:event/id "$event:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "!slot:example.org"
                                                        :message "raw room hello"
                                                        :reply-to/event-id "$parent:example.org"}
                                                ctx)
          (.then (fn [result]
                   (is (= {:room-id "!slot:example.org"
                           :message "raw room hello"
                           :opts {:client/id "client-1"
                                  :reply-to/event-id "$parent:example.org"}}
                          @sent*))
                   (is (= "!slot:example.org" (get-in result [:details :room/id])))
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
                                                        :room/id "!room:example.org"}}})
                :send-reaction! (fn [room-id event-id key opts]
                                  (reset! sent* {:room-id room-id
                                                 :event-id event-id
                                                 :key key
                                                 :opts opts})
                                  (js/Promise.resolve {:event/id "$reaction:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-reaction! deps {:target "ops"
                                                         :event/id "$event:example.org"
                                                         :key "👍"}
                                                 ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org"
                           :event-id "$event:example.org"
                           :key "👍"
                           :opts {:client/id "client-1"}}
                          @sent*))
                   (is (= {:content [{:type "text"
                                      :text "Sent Matrix reaction 👍 to $event:example.org in !room:example.org"}]
                           :details {:room/id "!room:example.org"
                                     :event/id "$reaction:example.org"
                                     :event/reacts-to-id "$event:example.org"
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
                                  (js/Promise.resolve {:event/id "$reaction:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-reaction! deps {:target "!slot:example.org"
                                                         :event/id "$event:example.org"
                                                         :key "👍"}
                                                 ctx)
          (.then (fn [result]
                   (is (= {:room-id "!slot:example.org"
                           :event-id "$event:example.org"
                           :key "👍"
                           :opts {:client/id "client-1"}}
                          @sent*))
                   (is (= "!slot:example.org" (get-in result [:details :room/id])))
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
                                                                     :room/id "!room:example.org"}}}
                                     :project {:project/id "project"
                                               :project/root "/work/project"
                                               :project/display-name "project"}
                                     :global-operators #{"@alice:example.org"}
                                     :bot-user-id "@bot:example.org"
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :pending-auto-replies* (atom [{:room-id "!slot:example.org"
                                                                    :event-id "$event:example.org"}])
                                     :heartbeat-id :interval-1
                                     :stream #js {:diagnostics (fn []
                                                                 #js {"connected" true
                                                                      "event/count" 3})}})
                :diagnostics* diagnostics*
                :read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :room/id "!room:example.org"}}})
                :health! (fn []
                           (js/Promise.resolve {:matrix/connected? true
                                                         :user/id "@bot:example.org"}))
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:project/id project-id
                                                    :slots [{:slot "A"
                                                             :room/id "!slot:example.org"
                                                             :room/name "project-A"
                                                             :client/id "matrix-relay-/work/project"
                                                             :state "leased"}]}))
                :list-rooms! (fn []
                               (js/Promise.resolve {:rooms [{:room/id "!slot:example.org"
                                                             :room/name "project-A"}]}))}
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
                          (get-in result [:details :diagnostics :recent-errors])))
                   (is (= {:running true
                           :client/id "matrix-relay-/work/project"
                           :slot "A"
                           :room/id "!slot:example.org"
                           :room/name "project-A"
                           :heartbeat/active? true
                           :stream/active? true
                           :pending-auto-replies/count 1}
                          (select-keys (get-in result [:details :relay])
                                       [:running :client/id :slot :room/id :room/name
                                        :heartbeat/active? :stream/active? :pending-auto-replies/count])))
                   (is (= {:connected true
                           :event/count 3}
                          (get-in result [:details :relay :stream/diagnostics])))
                   (is (= {:matrix/connected? true
                                     :user/id "@bot:example.org"}
                          (get-in result [:details :broker :health])))
                   (is (= [{:slot "A"
                            :room/id "!slot:example.org"
                            :room/name "project-A"
                            :client/id "matrix-relay-/work/project"
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
                              :project {:project/id "project"}
                              :slot "A"
                              :room-id "!old-slot:example.org"
                              :stream #js {:close (fn []
                                                    (swap! calls* conj [:close-old-stream]))}})
          deps {:relay-state* relay-state*
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix/connected? true}))
                :register-client! (fn [request]
                                    (swap! calls* conj [:register (:client/instance-id request)])
                                    (js/Promise.resolve {:client/id "client-2"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators []}))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve {:slot "B"
                                                      :room/id "!new-slot:example.org"
                                                      :room/name "project-B"}))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id (boolean (seq message)) opts])
                                (js/Promise.resolve {:event/id "$event:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [client-id reason]
                                      (swap! calls* conj [:unregister-client client-id reason])
                                      (js/Promise.resolve {}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms] :interval-2)
                :open-event-stream! (fn [_opts client-id _on-event]
                                      (swap! calls* conj [:open-event-stream client-id])
                                      #js {:close (fn [])})
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:project/id project-id :slots []}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:setStatus (fn [_id _status])}}
          pi #js {}]
      (-> (extension/execute-matrix-relay-control! deps {:action "restart"} pi ctx)
          (.then (fn [result]
                   (is (some #{[:close-old-stream]} @calls*))
                   (is (some #{[:release-slot "client-1" "!old-slot:example.org" "A"]} @calls*))
                   (is (some #{[:register extension/client-instance-id]} @calls*))
                   (is (= "client-2" (:client-id @relay-state*)))
                   (is (str/includes? (get-in result [:content 0 :text])
                                      "extension: running slot B project-B"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest tui-connect-reconnect-disconnect-commands-control-only-this-extension-client
  (async done
    (let [calls* (atom [])
          notifications* (atom [])
          start-count* (atom 0)
          relay-state* (atom nil)
          next-client-id (fn []
                           (let [n (swap! start-count* inc)]
                             (str "client-" n)))
          slot-for-client (fn [client-id]
                            (if (= "client-1" client-id)
                              {:slot "A"
                               :room/id "!slot-a:example.org"
                               :room/name "project-A"}
                              {:slot "B"
                               :room/id "!slot-b:example.org"
                               :room/name "project-B"}))
          deps {:relay-state* relay-state*
                :diagnostics* (atom {})
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix/connected? true
                                                :user/id "@bot:example.org"}))
                :register-client! (fn [request]
                                    (let [client-id (next-client-id)]
                                      (swap! calls* conj [:register (:client/instance-id request) client-id])
                                      (js/Promise.resolve {:client/id client-id
                                                           :heartbeat/seconds 30
                                                           :matrix/global-operators []})))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve (slot-for-client client-id)))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id (boolean (seq message)) opts])
                                (js/Promise.resolve {:event/id "$event:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [client-id reason]
                                      (swap! calls* conj [:unregister-client client-id reason])
                                      (js/Promise.resolve {}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms]
                                 (keyword (str "interval-" @start-count*)))
                :clear-interval! (fn [id]
                                   (swap! calls* conj [:clear-interval id]))
                :open-event-stream! (fn [_opts client-id _on-event]
                                      (swap! calls* conj [:open-event-stream client-id])
                                      #js {:close (fn []
                                                    (swap! calls* conj [:close-stream client-id]))})
                :list-slots! (fn [project-id]
                               (js/Promise.resolve {:project/id project-id :slots []}))
                :pi #js {}}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))
                            :setStatus (fn [_id _status])}}]
      (-> (extension/handle-command! deps "connect" ctx)
          (.then (fn [_]
                   (is (= "client-1" (:client-id @relay-state*)))
                   (is (str/includes? (ffirst @notifications*)
                                      "extension: running slot A project-A"))
                   (extension/handle-command! deps "reconnect" ctx)))
          (.then (fn [_]
                   (is (some #{[:close-stream "client-1"]} @calls*))
                   (is (some #{[:release-slot "client-1" "!slot-a:example.org" "A"]} @calls*))
                   (is (some #{[:unregister-client "client-1" "shutdown"]} @calls*))
                   (is (= "client-2" (:client-id @relay-state*)))
                   (is (str/includes? (first (last @notifications*))
                                      "extension: running slot B project-B"))
                   (extension/handle-command! deps "disconnect" ctx)))
          (.then (fn [_]
                   (is (some #{[:close-stream "client-2"]} @calls*))
                   (is (some #{[:release-slot "client-2" "!slot-b:example.org" "B"]} @calls*))
                   (is (some #{[:unregister-client "client-2" "shutdown"]} @calls*))
                   (is (nil? @relay-state*))
                   (is (str/includes? (first (last @notifications*))
                                      "extension: not running"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest tui-help-command-shows-cli-style-help
  (async done
    (let [notifications* (atom [])
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! {} "help" ctx)
          (.then (fn [_]
                   (is (= 1 (count @notifications*)))
                   (let [[message level] (first @notifications*)]
                     (is (= "info" level))
                     (is (str/includes? message "Usage:"))
                     (is (str/includes? message "/mr [command]"))
                     (is (str/includes? message "Commands:"))
                     (is (str/includes? message "connect"))
                     (is (str/includes? message "disconnect"))
                     (is (str/includes? message "reconnect"))
                     (is (str/includes? message "room bind <room> [alias]"))
                     (is (str/includes? message "send <alias-or-room-id> <message>")))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest tui-empty-command-shows-same-cli-style-help-as-help
  (async done
    (let [notifications* (atom [])
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! {} "" ctx)
          (.then (fn [_]
                   (let [blank-message (ffirst @notifications*)]
                     (reset! notifications* [])
                     (-> (extension/handle-command! {} "help" ctx)
                         (.then (fn [_]
                                  (is (= blank-message (ffirst @notifications*)))
                                  (done)))))))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest tui-status-shows-broker-identity-slot-room-and-listening-rooms
  (async done
    (let [notifications* (atom [])
          deps {:relay-state* (atom {:client-id "client-1"
                                     :project-config {:rooms {"ops" {:alias "ops"
                                                                     :room/id "!ops:example.org"
                                                                     :room/name "Ops Room"}
                                                             "live" {:alias "live"
                                                                      :room/id "!live:example.org"}}}
                                     :project {:project/id "project"}
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :heartbeat-id :heartbeat-1
                                     :stream #js {}})
                :health! (fn []
                           (js/Promise.resolve {:matrix/connected? true
                                                :user/id "@bot:example.org"}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "status" ctx)
          (.then (fn [_]
                   (is (= 1 (count @notifications*)))
                   (let [[message level] (first @notifications*)]
                     (is (= "info" level))
                     (is (str/includes? message "broker: Matrix connected as @bot:example.org"))
                     (is (str/includes? message "extension: connected to broker"))
                     (is (str/includes? message "slot: A project-A"))
                     (is (str/includes? message "slot room: !slot:example.org"))
                     (is (str/includes? message "listening rooms:"))
                     (is (str/includes? message "- !slot:example.org (slot project-A)"))
                     (is (str/includes? message "- !ops:example.org (ops / Ops Room)"))
                     (is (str/includes? message "- !live:example.org (live)")))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest tui-status-reports-matrix-space-setup-errors
  (async done
    (let [notifications* (atom [])
          deps {:relay-state* (atom {:client-id "client-1"
                                     :project-config {}
                                     :project {:project/id "project"}
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :heartbeat-id :heartbeat-1
                                     :stream #js {}})
                :health! (fn []
                           (js/Promise.resolve {:matrix/connected? true
                                                :user/id "@bot:example.org"
                                                :matrix/space {:status "error"
                                                               :space/enabled? true
                                                               :error {:code "matrix_space_setup_failed"
                                                                       :message "Could not join configured Matrix space."}}}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "status" ctx)
          (.then (fn [_]
                   (let [[message level] (first @notifications*)]
                     (is (= "warning" level))
                     (is (str/includes? message "broker: Matrix connected as @bot:example.org"))
                     (is (str/includes? message "space: error Could not join configured Matrix space.")))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest tui-status-reports-closed-event-stream-as-inactive
  (async done
    (let [notifications* (atom [])
          deps {:relay-state* (atom {:client-id "client-1"
                                     :project-config {}
                                     :project {:project/id "project"}
                                     :slot "A"
                                     :room-id "!slot:example.org"
                                     :room-name "project-A"
                                     :heartbeat-id :heartbeat-1
                                     :stream #js {:diagnostics (fn []
                                                                 (clj->js {:stream/closed? true
                                                                           :stream/close-reason "response-close"}))}})
                :health! (fn []
                           (js/Promise.resolve {:matrix/connected? true
                                                :user/id "@bot:example.org"}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "status" ctx)
          (.then (fn [_]
                   (let [[message level] (first @notifications*)]
                     (is (= "warning" level))
                     (is (str/includes? message "stream: inactive")))
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
                                                        :room/id "!room:example.org"}}})
                :send-message! (fn [room-id message]
                                (reset! sent* {:room-id room-id :message message})
                                (js/Promise.resolve {:event/id "$event:example.org"}))}
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
                                                        :room/id "!room:example.org"
                                                        :mode "all"}}})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix/connected? true
                                                         :user/id "@bot:example.org"}))
                :register-client! (fn [request]
                                    (swap! calls* conj [:register request])
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :event-stream/path "/v1/clients/client-1/events"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators ["@alice:example.org"]}))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id message opts])
                                (js/Promise.resolve {:event/id "$start:example.org"}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeat/seconds 30}))
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
                           [:register {:client/instance-id extension/client-instance-id
                                       :protocol/version 1
                                       :project {:project/root "/work/project"
                                                 :project/id "project"}
                                       :subscriptions {:rooms ["!room:example.org"]}}]
                           [:acquire-slot "client-1" {:project/id "project" :project/display-name "project"} ["@alice:example.org"]]
                           [:update-subscriptions "client-1" ["!room:example.org" "!slot:example.org"]]
                           [:send-message "!slot:example.org" (:last-start-banner relay-state) {:client/id "client-1"}]
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

(deftest event-stream-close-marks-stream-inactive-and-reopens-without-new-slot
  (async done
    (let [calls* (atom [])
          close-handlers* (atom [])
          relay-state* (atom nil)
          stream-id* (atom 0)
          deps {:relay-state* relay-state*
                :event-stream-reconnect-ms 0
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix/connected? true
                                                :user/id "@bot:example.org"}))
                :register-client! (fn [_request]
                                    (swap! calls* conj [:register])
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators []}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (swap! calls* conj [:acquire-slot])
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [_client-id _rooms]
                                         (swap! calls* conj [:update-subscriptions])
                                         (js/Promise.resolve {:rooms []}))
                :send-message! (fn [_room-id _message _opts]
                                (swap! calls* conj [:send-message])
                                (js/Promise.resolve {:event/id "$start:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :set-timeout! (fn [f ms]
                                (swap! calls* conj [:set-timeout ms])
                                (f)
                                :timeout-1)
                :open-event-stream! (fn [opts client-id _on-event]
                                      (let [stream-id (swap! stream-id* inc)
                                            closed? (atom false)
                                            close-requested? (atom false)
                                            stream #js {:close (fn []
                                                                 (reset! close-requested? true)
                                                                 (reset! closed? true))
                                                        :diagnostics (fn []
                                                                       (clj->js {:stream/id stream-id
                                                                                 :stream/closed? @closed?
                                                                                 :close/requested? @close-requested?}))}]
                                        (swap! calls* conj [:open-event-stream client-id stream-id])
                                        (swap! close-handlers* conj (:on-close opts))
                                        stream))}
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [_message _level])
                            :setStatus (fn [_id _status])}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [_]
                   (let [first-stream (:stream @relay-state*)
                         first-close-handler (first @close-handlers*)]
                     (is (= [[:health]
                             [:register]
                             [:acquire-slot]
                             [:update-subscriptions]
                             [:send-message]
                             [:open-event-stream "client-1" 1]]
                            @calls*))
                     (first-close-handler {:stream/closed? true
                                           :stream/close-reason "response-close"})
                     (is (= [[:health]
                             [:register]
                             [:acquire-slot]
                             [:update-subscriptions]
                             [:send-message]
                             [:open-event-stream "client-1" 1]
                             [:set-timeout 0]
                             [:open-event-stream "client-1" 2]]
                            @calls*))
                     (is (not= first-stream (:stream @relay-state*)))
                     (is (= 2 (count @close-handlers*)))
                     (done))))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest event-stream-requested-close-does-not-reopen
  (async done
    (let [calls* (atom [])
          close-handler* (atom nil)
          relay-state* (atom nil)
          stream-id* (atom 0)
          deps {:relay-state* relay-state*
                :event-stream-reconnect-ms 0
                :read-project-config! (fn [_cwd] {})
                :health! (fn [] (js/Promise.resolve {:matrix/connected? true
                                                     :user/id "@bot:example.org"}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators []}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [_client-id _rooms]
                                         (js/Promise.resolve {:rooms []}))
                :send-message! (fn [_room-id _message _opts]
                                (js/Promise.resolve {:event/id "$start:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :set-timeout! (fn [f ms]
                                (swap! calls* conj [:set-timeout ms])
                                (f)
                                :timeout-1)
                :open-event-stream! (fn [opts client-id _on-event]
                                      (let [stream-id (swap! stream-id* inc)]
                                        (swap! calls* conj [:open-event-stream client-id stream-id])
                                        (reset! close-handler* (:on-close opts))
                                        #js {:close (fn [])
                                             :diagnostics (fn []
                                                            (clj->js {:stream/id stream-id}))}))}
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [_message _level])
                            :setStatus (fn [_id _status])}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [_]
                   (@close-handler* {:stream/closed? true
                                     :close/requested? true
                                     :stream/close-reason "response-close"})
                   (is (= [[:open-event-stream "client-1" 1]] @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest event-stream-stale-reconnect-does-not-open-when-stream-already-active
  (async done
    (let [calls* (atom [])
          close-handler* (atom nil)
          reconnect!* (atom nil)
          relay-state* (atom nil)
          stream-id* (atom 0)
          deps {:relay-state* relay-state*
                :event-stream-reconnect-ms 0
                :read-project-config! (fn [_cwd] {})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix/connected? true
                                                :user/id "@bot:example.org"}))
                :register-client! (fn [_request]
                                    (swap! calls* conj [:register])
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators []}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (swap! calls* conj [:acquire-slot])
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [_client-id _rooms]
                                         (swap! calls* conj [:update-subscriptions])
                                         (js/Promise.resolve {:rooms []}))
                :send-message! (fn [_room-id _message _opts]
                                (swap! calls* conj [:send-message])
                                (js/Promise.resolve {:event/id "$start:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :set-timeout! (fn [f ms]
                                (swap! calls* conj [:set-timeout ms])
                                (reset! reconnect!* f)
                                :timeout-1)
                :open-event-stream! (fn [opts client-id _on-event]
                                      (let [stream-id (swap! stream-id* inc)]
                                        (swap! calls* conj [:open-event-stream client-id stream-id])
                                        (reset! close-handler* (:on-close opts))
                                        #js {:close (fn [])
                                             :diagnostics (fn []
                                                            (clj->js {:stream/id stream-id}))}))}
          replacement-stream #js {:diagnostics (fn []
                                                 (clj->js {:stream/id "replacement"}))}
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [_message _level])
                            :setStatus (fn [_id _status])}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [_]
                   (@close-handler* {:stream/closed? true
                                     :stream/close-reason "response-close"})
                   (swap! relay-state* assoc :stream replacement-stream)
                   (@reconnect!*)
                   (is (= [[:health]
                           [:register]
                           [:acquire-slot]
                           [:update-subscriptions]
                           [:send-message]
                           [:open-event-stream "client-1" 1]
                           [:set-timeout 0]]
                          @calls*))
                   (is (identical? replacement-stream (:stream @relay-state*)))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest session-start-loads-room-delivery-modes-before-opening-event-stream
  (async done
    (let [calls* (atom [])
          deps {:read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :room/id "!room:example.org"
                                                        :mode "all"}}})
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix/connected? true
                                                         :user/id "@bot:example.org"}))
                :register-client! (fn [_request]
                                    (swap! calls* conj [:register])
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :event-stream/path "/v1/clients/client-1/events"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators ["@alice:example.org"]}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (swap! calls* conj [:acquire-slot])
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (swap! calls* conj [:update-subscriptions rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :get-room-delivery-mode! (fn [client-id room-id]
                                          (swap! calls* conj [:get-room-delivery-mode client-id room-id])
                                          (js/Promise.resolve {:room/id room-id
                                                               :room/default-delivery-mode (when (= "!slot:example.org" room-id)
                                                                                      "steer")}))
                :send-message! (fn [room-id _message _opts]
                                (swap! calls* conj [:send-message room-id])
                                (js/Promise.resolve {:event/id "$start:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
                :set-interval! (fn [_f _ms] :interval-1)
                :open-event-stream! (fn [_opts client-id _on-event]
                                      (swap! calls* conj [:open-event-stream client-id])
                                      #js {:close (fn [])})}
          pi #js {:sendUserMessage (fn [_message])}
          ctx #js {:cwd "/work/project"
                   :ui #js {:setStatus (fn [_id _status])}}]
      (-> (extension/start-relay! deps pi ctx)
          (.then (fn [relay-state]
                   (is (= [[:health]
                           [:register]
                           [:acquire-slot]
                           [:update-subscriptions ["!room:example.org" "!slot:example.org"]]
                           [:get-room-delivery-mode "client-1" "!room:example.org"]
                           [:get-room-delivery-mode "client-1" "!slot:example.org"]
                           [:send-message "!slot:example.org"]
                           [:open-event-stream "client-1"]]
                          @calls*))
                   (is (= {"!slot:example.org" {:delivery-mode "steer" :source "broker"}}
                          @(:room-delivery-modes* relay-state)))
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
                           (js/Promise.resolve {:matrix/connected? true
                                                         :user/id "@bot:example.org"}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators ["@alice:example.org"]}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! sent* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$sent:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
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
                     :room/id "!slot:example.org"
                     :event/id "$status:example.org"
                             :event/sender "@alice:example.org"
                             :event/sender-is-bot? false
                             :event/timestamp "2026-05-16T12:34:56Z"
                             :event/text "/status"})
                   (is (str/includes? (:message (last @sent*)) "default delivery mode: follow-up (system-default)"))
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
                           (js/Promise.resolve {:matrix/connected? true
                                                         :user/id "@bot:example.org"}))
                :register-client! (fn [_request]
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators ["@alice:example.org"]}))
                :acquire-slot! (fn [_client-id _project _invite]
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [_client-id rooms]
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [_room-id _message _opts]
                                (js/Promise.resolve {:event/id "$sent:example.org"}))
                :heartbeat! (fn [_client-id]
                              (js/Promise.resolve {:heartbeat/seconds 30}))
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
                     :room/id "!slot:example.org"
                     :event/id "$event:example.org"
                             :event/sender "@alice:example.org"
                             :event/sender-is-bot? false
                             :event/timestamp "2026-05-16T12:34:56Z"
                             :event/text "ordinary slot prompt"})
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
                           (js/Promise.resolve {:matrix/connected? true
                                                         :user/id "@bot:example.org"}))
                :register-client! (fn [request]
                                    (swap! calls* conj [:register request])
                                    (js/Promise.resolve {:client/id "client-1"
                                                         :heartbeat/seconds 30
                                                         :matrix/global-operators ["@alice:example.org"]}))
                :acquire-slot! (fn [client-id project invite]
                                 (swap! calls* conj [:acquire-slot client-id project invite])
                                 (js/Promise.resolve {:slot "A"
                                                      :room/id "!slot:example.org"
                                                      :room/name "project-A"}))
                :update-subscriptions! (fn [client-id rooms]
                                         (swap! calls* conj [:update-subscriptions client-id rooms])
                                         (js/Promise.resolve {:rooms rooms}))
                :send-message! (fn [room-id message opts]
                                (swap! calls* conj [:send-message room-id (boolean (seq message)) opts])
                                (js/Promise.resolve {:event/id "$start:example.org"}))
                :heartbeat! (fn [client-id]
                              (swap! calls* conj [:heartbeat client-id])
                              (js/Promise.resolve {:heartbeat/seconds 30}))
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
                           [:register {:client/instance-id extension/client-instance-id
                                       :protocol/version 1
                                       :project {:project/root "/work/project"
                                                 :project/id "project"}
                                       :subscriptions {:rooms []}}]
                           [:acquire-slot "client-1" {:project/id "project" :project/display-name "project"} ["@alice:example.org"]]
                           [:update-subscriptions "client-1" ["!slot:example.org"]]
                           [:send-message "!slot:example.org" true {:client/id "client-1"}]
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
                                (js/Promise.resolve {:event/id "$end:example.org"}))
                :release-slot! (fn [client-id room-id slot]
                                 (swap! calls* conj [:release-slot client-id room-id slot])
                                 (js/Promise.resolve {:released true}))
                :unregister-client! (fn [client-id reason]
                                      (swap! calls* conj [:unregister-client client-id reason])
                                      (js/Promise.resolve {}))}
          relay-state {:client-id "client-1"
                       :project {:project/id "project"}
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
                           [:send-message "!slot:example.org" true {:client/id "client-1"}]
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
                       :project {:project/id "project"}
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
               :room/id "!slot:example.org"
               :event/id "$slot-event:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "no bot mention needed in slot rooms"}]
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
               :room/id "!slot:example.org"
               :event/id "$slot-event:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "please respond"}]
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
                                                      :room/id "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :pending-auto-replies* pending*}
        event {:type "matrix.message"
               :room/id "!room:example.org"
               :event/id "$project-event:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "project prompt"}]
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
               :room/id "!slot:example.org"
               :event/id "$slot-event:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "please respond"}]
    (with-redefs [config/default-delivery-mode "reject"]
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
                     :project {:project/id "project"}
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$status:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "//status"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= [] @sent*))
    (is (= 1 (count @acks*)))
    (is (= {:client/id "client-1"
            :reply-to/event-id "$status:example.org"}
           (select-keys (:opts (first @acks*)) [:client/id :reply-to/event-id])))
    (let [ack (first @acks*)
          formatted-body (str (get-in ack [:opts :formatted-body]))]
      (is (str/includes? (:message ack) "pi-matrix-relay status"))
      (is (str/includes? (:message ack) "slot: A project-A"))
      (is (str/includes? (:message ack) "default delivery mode: follow-up (system-default)"))
      (is (str/includes? (:message ack) "prompt mode: all (slot-default)"))
      (is (str/includes? (:message ack) "model: openai-codex/gpt-5.5"))
      (is (str/includes? (:message ack) "context: 123456 tokens (45%/272k)"))
      (is (str/includes? (:message ack) "usage: ↑360.0k ↓14.0k $4.591"))
      (is (str/includes? formatted-body "<h3>pi-matrix-relay status</h3>"))
      (is (str/includes? formatted-body "<table>"))
      (is (str/includes? formatted-body "<th>Project</th><td><code>project</code></td>"))
      (is (str/includes? formatted-body "<th>Slot</th><td><code>A</code> project-A</td>"))
      (is (str/includes? formatted-body "<th>Room</th><td><code>!slot:example.org</code></td>"))
      (is (str/includes? formatted-body "<th>Prompt mode</th><td><code>all</code> <em>slot-default</em></td>"))
      (is (str/includes? formatted-body "<th>Default delivery mode</th><td><code>follow-up</code> <em>system-default</em></td>"))
      (is (str/includes? formatted-body "<th>Heartbeat</th><td>active</td>"))
      (is (str/includes? formatted-body "<th>Stream</th><td>active</td>"))
      (is (str/includes? formatted-body "<th>Model</th><td><code>openai-codex/gpt-5.5</code></td>"))
      (is (str/includes? formatted-body "<th>Context</th><td>123456 tokens (45%/272k)</td>"))
      (is (str/includes? formatted-body "<th>Usage</th><td>↑360.0k ↓14.0k $4.591</td>")))))

(deftest matrix-help-command-lists-commands-and-prefixes
  (let [sent* (atom [])
        acks* (atom [])
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:project/id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$help:example.org"
               :event/sender "@alice:example.org"
               :event/timestamp "2026-05-16T12:34:56Z"
               :event/text "/help"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= [] @sent*))
    (let [ack (first @acks*)
          formatted-body (str (get-in ack [:opts :formatted-body]))]
      (is (= "!slot:example.org" (:room-id ack)))
      (is (= {:client/id "client-1"
              :reply-to/event-id "$help:example.org"}
             (select-keys (:opts ack) [:client/id :reply-to/event-id])))
      (is (str/includes? (:message ack) "Matrix relay commands"))
      (is (str/includes? (:message ack) "Prefixes: / or !"))
      (is (str/includes? (:message ack) "!status"))
      (is (str/includes? (:message ack) "!prompt <mode>"))
      (is (not (str/includes? (:message ack) "/status or !status")))
      (is (str/includes? (:message ack) "Use !help <command> for details."))
      (is (str/includes? formatted-body "<h3>Matrix relay commands</h3>"))
      (is (str/includes? formatted-body "<table>"))
      (is (str/includes? formatted-body "<code>!status</code>"))
      (is (str/includes? formatted-body "<code>!prompt &lt;mode&gt;</code>"))
      (is (not (str/includes? formatted-body "<code>/status</code> or <code>!status</code>")))
      (is (str/includes? formatted-body "<code>!help &lt;command&gt;</code>")))))

(deftest matrix-help-command-shows-subcommand-help-and-accepts-bang-prefix
  (let [acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:project/id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$help-steer:example.org"
               :event/sender "@alice:example.org"
               :event/timestamp "2026-05-16T12:34:56Z"
               :event/text "!help steer"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @acks*)))
    (let [ack (first @acks*)
          formatted-body (str (get-in ack [:opts :formatted-body]))]
      (is (str/includes? (:message ack) "Matrix relay command: steer"))
      (is (str/includes? (:message ack) "Usage: !steer [message]"))
      (is (not (str/includes? (:message ack) "/steer [message] or !steer [message]")))
      (is (str/includes? (:message ack) "With a message, steer it into the current Pi turn."))
      (is (str/includes? formatted-body "<h3>Matrix relay command: <code>steer</code></h3>"))
      (is (str/includes? formatted-body "<code>!steer [message]</code>"))
      (is (not (str/includes? formatted-body "<code>/steer [message]</code> or <code>!steer [message]</code>")))
      (is (str/includes? formatted-body "<li>With a message, steer it into the current Pi turn.</li>")))))

(deftest bang-prefixed-matrix-status-command-sends-room-ack
  (let [acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:project/id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$bang-status:example.org"
               :event/sender "@alice:example.org"
               :event/timestamp "2026-05-16T12:34:56Z"
               :event/text "!status"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "pi-matrix-relay status"))))

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
                     :project {:project/id "project"}
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$status:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/status"}]
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

(deftest matrix-status-command-reports-broker-prompt-and-delivery-mode-sources
  (let [acks* (atom [])
        pi #js {:sendUserMessage (fn [_message _options])}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :project {:project/id "project"}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"
                     :room-delivery-modes* (atom {"!slot:example.org" {:delivery-mode "steer"
                                                                        :source "broker"}})
                     :room-prompt-modes* (atom {"!slot:example.org" {:mode "commands-only"
                                                                      :source "broker"}})}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$status:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/status"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (str/includes? (:message (first @acks*)) "prompt mode: commands-only (broker)"))
    (is (str/includes? (:message (first @acks*)) "default delivery mode: steer (broker)"))))

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
                     :project {:project/id "project"}
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$status:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/status"}]
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$abort:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/abort"}]
    (try
      (extension/handle-broker-event! deps pi ctx relay-state event)
      (catch js/Error err
        (reset! thrown* err)))
    (is (nil? @thrown*) (some-> @thrown* .-message))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "Remote command /abort failed: abort failed"))
    (is (= {:client/id "client-1"
            :reply-to/event-id "$abort:example.org"}
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$abort:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/abort"}]
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$steer:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/steer focus on the failing test"}]
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$follow-up:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/follow-up run the next check"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/includes? (:message (first @sent*)) "run the next check"))
    (is (= {:deliverAs "followUp"} (:options (first @sent*))))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "Follow-up message queued"))))

(deftest bare-steer-command-persists-room-default-before-acking
  (async done
    (let [sent* (atom [])
          acks* (atom [])
          delivery-modes* (atom {})
          persist* (atom nil)
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
                       :room-delivery-modes* delivery-modes*
                       :pending-auto-replies* (atom [])}
          deps {:set-room-delivery-mode! (fn [client-id room-id mode updated-by-user]
                                          (reset! persist* {:client-id client-id
                                                           :room-id room-id
                                                           :mode mode
                                                           :updated-by-user updated-by-user})
                                          (js/Promise.resolve {:room/id room-id
                                                               :room/default-delivery-mode mode}))
                :send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$ack:example.org"}))}
          steer-event {:type "matrix.message"
                       :room/id "!slot:example.org"
                       :event/id "$set-steer:example.org"
                               :event/sender "@alice:example.org"
                               :event/timestamp "2026-05-16T12:34:56Z"
                               :event/text "/steer"}
          prompt-event {:type "matrix.message"
                        :room/id "!slot:example.org"
                        :event/id "$next:example.org"
                                :event/sender "@alice:example.org"
                                :event/timestamp "2026-05-16T12:34:57Z"
                                :event/text "ordinary slot prompt"}]
      (-> (js/Promise.resolve (extension/handle-broker-event! deps pi ctx relay-state steer-event))
          (.then (fn [_]
                   (is (= {:client-id "client-1"
                           :room-id "!slot:example.org"
                           :mode "steer"
                           :updated-by-user "@alice:example.org"}
                          @persist*))
                   (is (= {"!slot:example.org" {:delivery-mode "steer" :source "broker"}}
                          @delivery-modes*))
                   (is (= [] @sent*))
                   (is (str/includes? (:message (first @acks*))
                                      "Default delivery mode for this room is now steer"))
                   (extension/handle-broker-event! deps pi ctx relay-state prompt-event)
                   (is (= 1 (count @sent*)))
                   (is (= {:deliverAs "steer"} (:options (first @sent*))))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest bare-follow-up-and-reject-commands-persist-room-defaults
  (async done
    (let [acks* (atom [])
          delivery-modes* (atom {"!slot:example.org" {:delivery-mode "steer" :source "broker"}})
          persisted* (atom [])
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"
                   :isIdle (fn [] false)}
          relay-state {:client-id "client-1"
                       :project-config {}
                       :global-operators #{"@alice:example.org"}
                       :bot-user-id "@bot:example.org"
                       :slot "A"
                       :room-id "!slot:example.org"
                       :room-name "project-A"
                       :room-delivery-modes* delivery-modes*
                       :pending-auto-replies* (atom [])}
          deps {:set-room-delivery-mode! (fn [_client-id room-id mode _updated-by-user]
                                          (swap! persisted* conj mode)
                                          (js/Promise.resolve {:room/id room-id
                                                               :room/default-delivery-mode mode}))
                :send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$ack:example.org"}))}
          event-for (fn [event-id text]
                      {:type "matrix.message"
                       :room/id "!slot:example.org"
                       :event/id event-id
                               :event/sender "@alice:example.org"
                               :event/timestamp "2026-05-16T12:34:56Z"
                               :event/text text})]
      (-> (js/Promise.resolve (extension/handle-broker-event! deps pi ctx relay-state (event-for "$follow" "/follow-up")))
          (.then (fn [_]
                   (extension/handle-broker-event! deps pi ctx relay-state (event-for "$reject" "/reject"))))
          (.then (fn [_]
                   (is (= ["follow-up" "reject"] @persisted*))
                   (is (= {"!slot:example.org" {:delivery-mode "reject" :source "broker"}}
                          @delivery-modes*))
                   (is (= ["Default delivery mode for this room is now follow-up."
                           "Default delivery mode for this room is now reject."]
                          (mapv :message @acks*)))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest bare-delivery-mode-command-reports-broker-failure-without-changing-cache
  (async done
    (let [acks* (atom [])
          delivery-modes* (atom {"!slot:example.org" {:delivery-mode "steer" :source "broker"}})
          pi #js {:sendUserMessage (fn [_message _options])}
          ctx #js {:cwd "/work/project"}
          relay-state {:client-id "client-1"
                       :project-config {}
                       :global-operators #{"@alice:example.org"}
                       :bot-user-id "@bot:example.org"
                       :slot "A"
                       :room-id "!slot:example.org"
                       :room-name "project-A"
                       :room-delivery-modes* delivery-modes*}
          deps {:set-room-delivery-mode! (fn [_client-id _room-id _mode _updated-by-user]
                                          (js/Promise.reject (js/Error. "broker write failed")))
                :send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$ack:example.org"}))}
          event {:type "matrix.message"
                 :room/id "!slot:example.org"
                 :event/id "$set-steer:example.org"
                         :event/sender "@alice:example.org"
                         :event/timestamp "2026-05-16T12:34:56Z"
                         :event/text "/follow-up"}]
      (-> (js/Promise.resolve (extension/handle-broker-event! deps pi ctx relay-state event))
          (.then (fn [_]
                   (is (= {"!slot:example.org" {:delivery-mode "steer" :source "broker"}}
                          @delivery-modes*))
                   (is (str/includes? (:message (first @acks*))
                                      "Default delivery mode update failed: broker write failed"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest matrix-prompt-command-persists-room-prompt-mode-before-acking
  (async done
    (let [sent* (atom [])
          acks* (atom [])
          prompt-modes* (atom {"!room:example.org" {:mode "commands-only" :source "broker"}})
          persist* (atom nil)
          pi #js {:sendUserMessage (fn [message options]
                                     (swap! sent* conj {:message message
                                                        :options (some-> options (js->clj :keywordize-keys true))}))}
          ctx #js {:cwd "/work/project"
                   :isIdle (fn [] true)}
          relay-state {:client-id "client-1"
                       :project-config {:rooms {"ops" {:alias "ops"
                                                        :room/id "!room:example.org"
                                                        :mode "commands-only"}}}
                       :global-operators #{"@alice:example.org"}
                       :bot-user-id "@bot:example.org"
                       :room-prompt-modes* prompt-modes*}
          deps {:set-room-prompt-mode! (fn [client-id room-id mode updated-by-user]
                                         (reset! persist* {:client-id client-id
                                                          :room-id room-id
                                                          :mode mode
                                                          :updated-by-user updated-by-user})
                                         (js/Promise.resolve {:room/id room-id
                                                              :room/prompt-mode mode}))
                :send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$ack:example.org"}))}
          prompt-event {:type "matrix.message"
                        :room/id "!room:example.org"
                        :event/id "$set-prompt:example.org"
                        :event/sender "@alice:example.org"
                        :event/timestamp "2026-05-16T12:34:56Z"
                        :event/text "!prompt all"}
          message-event {:type "matrix.message"
                         :room/id "!room:example.org"
                         :event/id "$ordinary:example.org"
                         :event/sender "@alice:example.org"
                         :event/timestamp "2026-05-16T12:34:57Z"
                         :event/text "ordinary project prompt"}]
      (-> (js/Promise.resolve (extension/handle-broker-event! deps pi ctx relay-state prompt-event))
          (.then (fn [_]
                   (is (= {:client-id "client-1"
                           :room-id "!room:example.org"
                           :mode "all"
                           :updated-by-user "@alice:example.org"}
                          @persist*))
                   (is (= {"!room:example.org" {:mode "all" :source "broker"}}
                          @prompt-modes*))
                   (is (= 1 (count @acks*)))
                   (is (str/includes? (:message (first @acks*))
                                      "Prompt mode for this room is now all"))
                   (extension/handle-broker-event! deps pi ctx relay-state message-event)
                   (is (= 1 (count @sent*)))
                   (is (str/includes? (:message (first @sent*)) "ordinary project prompt"))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest invalid-matrix-prompt-command-acks-usage-without-changing-cache
  (let [acks* (atom [])
        prompt-modes* (atom {"!room:example.org" {:mode "commands-only" :source "broker"}})
        pi #js {:sendUserMessage (fn [_message _options])}
        ctx #js {:cwd "/work/project"}
        relay-state {:client-id "client-1"
                     :project-config {:rooms {"ops" {:alias "ops"
                                                      :room/id "!room:example.org"
                                                      :mode "commands-only"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :room-prompt-modes* prompt-modes*}
        deps {:set-room-prompt-mode! (fn [& _]
                                       (throw (js/Error. "should not persist invalid mode")))
              :send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!room:example.org"
               :event/id "$bad-prompt:example.org"
               :event/sender "@alice:example.org"
               :event/timestamp "2026-05-16T12:34:56Z"
               :event/text "!prompt interrupt"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= {"!room:example.org" {:mode "commands-only" :source "broker"}}
           @prompt-modes*))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*))
                       "Usage: !prompt <mode>"))))

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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$abort:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/abort"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (true? @aborted?))
    (is (str/includes? (:message (first @acks*)) "Abort requested"))
    (is (= {:client/id "client-1"
            :reply-to/event-id "$abort:example.org"}
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$compact:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/compact focus on matrix relay changes"}]
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
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$compact:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/compact"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (false? @compact-called?))
    (is (str/includes? (:message (first @acks*)) "Nothing to compact"))))

(deftest matrix-new-command-queues-command-context-bridge
  (let [acks* (atom [])
        sent* (atom [])
        pending* (atom {})
        pi #js {:sendUserMessage (fn [message options]
                                   (swap! sent* conj {:message message
                                                      :options (some-> options (js->clj :keywordize-keys true))}))}
        ctx #js {:cwd "/work/project"}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:pending-new-sessions* pending*
              :send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$new:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/new"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (= 1 (count @sent*)))
    (is (str/starts-with? (:message (first @sent*)) "/matrix-relay __new-session "))
    (is (= {:deliverAs "followUp"} (:options (first @sent*))))
    (let [request-id (last (str/split (:message (first @sent*)) #"\s+"))]
      (is (= {:room-id "!slot:example.org"
              :event-id "$new:example.org"}
             (select-keys (get @pending* request-id) [:room-id :event-id]))))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "New session requested"))
    (is (= {:client/id "client-1"
            :reply-to/event-id "$new:example.org"}
           (:opts (first @acks*))))))

(deftest matrix-new-command-reports-when-command-bridge-is-unavailable
  (let [acks* (atom [])
        pi #js {}
        ctx #js {:cwd "/work/project"}
        relay-state {:client-id "client-1"
                     :project-config {}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:pending-new-sessions* (atom {})
              :send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}
        event {:type "matrix.message"
               :room/id "!slot:example.org"
               :event/id "$new:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "/new"}]
    (extension/handle-broker-event! deps pi ctx relay-state event)
    (is (str/includes? (:message (first @acks*)) "New session cannot be queued"))))

(deftest internal-new-session-command-uses-command-context-and-sends-result-ack
  (async done
    (let [new-session-called? (atom false)
          wait-called? (atom false)
          acks* (atom [])
          pending* (atom {"req-1" {:room-id "!slot:example.org"
                                   :event-id "$new:example.org"}})
          deps {:pending-new-sessions* pending*
                :send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$ack:example.org"}))}
          ctx #js {:cwd "/work/project"
                   :waitForIdle (fn []
                                  (reset! wait-called? true)
                                  (js/Promise.resolve nil))
                   :newSession (fn [options]
                                 (reset! new-session-called? true)
                                 (-> ((.-withSession ^js options) #js {})
                                     (.then (fn [_]
                                              #js {:cancelled false}))))}]
      (-> (extension/handle-command! deps "__new-session req-1" ctx)
          (.then (fn [_]
                   (is (true? @wait-called?))
                   (is (true? @new-session-called?))
                   (is (= {} @pending*))
                   (is (= [{:room-id "!slot:example.org"
                            :message "New session started."
                            :opts {:reply-to/event-id "$new:example.org"}}]
                          @acks*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest internal-new-session-command-reports-cancelled-session
  (async done
    (let [acks* (atom [])
          pending* (atom {"req-1" {:room-id "!slot:example.org"
                                   :event-id "$new:example.org"}})
          deps {:pending-new-sessions* pending*
                :send-message! (fn [room-id message opts]
                                (swap! acks* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$ack:example.org"}))}
          ctx #js {:cwd "/work/project"
                   :waitForIdle (fn []
                                  (js/Promise.resolve nil))
                   :newSession (fn [_options]
                                 (js/Promise.resolve #js {:cancelled true}))}]
      (-> (extension/handle-command! deps "__new-session req-1" ctx)
          (.then (fn [_]
                   (is (= [{:room-id "!slot:example.org"
                            :message "New session cancelled."
                            :opts {:reply-to/event-id "$new:example.org"}}]
                          @acks*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest agent-end-sends-automatic-reply-for-pending-slot-prompt
  (async done
    (let [sent* (atom [])
          pending* (atom [{:room-id "!slot:example.org"
                           :event-id "$slot-event:example.org"}])
          deps {:send-message! (fn [room-id message opts]
                                (swap! sent* conj {:room-id room-id
                                                   :message message
                                                   :opts opts})
                                (js/Promise.resolve {:event/id "$reply:example.org"}))}
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
                            :opts {:client/id "client-1"
                                   :reply-to/event-id "$slot-event:example.org"}}]
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
                    :room/id "!slot:example.org"
                    :event/id "$bot:example.org"
                            :event/sender "@bot:example.org"
                            :event/sender-is-bot? true
                            :event/timestamp "2026-05-16T12:34:56Z"
                            :event/text "bot echo"}
                   {:type "matrix.message"
                    :room/id "!slot:example.org"
                    :event/id "$mallory:example.org"
                            :event/sender "@mallory:example.org"
                            :event/timestamp "2026-05-16T12:34:56Z"
                            :event/text "unauthorized"}]]
      (extension/handle-broker-event! {} pi ctx relay-state event))
    (is (= [] @sent*))))

(deftest project-room-mentions-mode-still-requires-a-bot-mention
  (let [sent* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:project-config {:rooms {"ops" {:alias "ops"
                                                      :room/id "!room:example.org"
                                                      :mode "mentions"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}]
    (extension/handle-broker-event!
     {}
     pi
     ctx
     relay-state
     {:type "matrix.message"
      :room/id "!room:example.org"
      :event/id "$ignored:example.org"
              :event/sender "@alice:example.org"
              :event/timestamp "2026-05-16T12:34:56Z"
              :event/text "ordinary project chatter"})
    (extension/handle-broker-event!
     {}
     pi
     ctx
     relay-state
     {:type "matrix.message"
      :room/id "!room:example.org"
      :event/id "$mentioned:example.org"
              :event/sender "@alice:example.org"
              :event/timestamp "2026-05-16T12:34:57Z"
              :event/text "@bot please inspect this"})
    (is (= 1 (count @sent*)))
    (is (str/includes? (first @sent*) "@bot please inspect this"))))

(deftest project-room-commands-only-mode-processes-commands-but-ignores-prompts
  (let [sent* (atom [])
        acks* (atom [])
        pi #js {:sendUserMessage (fn [message]
                                   (swap! sent* conj message))}
        ctx #js {:cwd "/work/project"
                 :isIdle (fn [] true)}
        relay-state {:client-id "client-1"
                     :project-config {:rooms {"ops" {:alias "ops"
                                                      :room/id "!room:example.org"
                                                      :mode "commands-only"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"
                     :slot "A"
                     :room-id "!slot:example.org"
                     :room-name "project-A"}
        deps {:send-message! (fn [room-id message opts]
                              (swap! acks* conj {:room-id room-id
                                                 :message message
                                                 :opts opts})
                              (js/Promise.resolve {:event/id "$ack:example.org"}))}]
    (extension/handle-broker-event!
     deps
     pi
     ctx
     relay-state
     {:type "matrix.message"
      :room/id "!room:example.org"
      :event/id "$ignored:example.org"
      :event/sender "@alice:example.org"
      :event/timestamp "2026-05-16T12:34:56Z"
      :event/text "@bot this would normally mention you"})
    (extension/handle-broker-event!
     deps
     pi
     ctx
     relay-state
     {:type "matrix.message"
      :room/id "!room:example.org"
      :event/id "$status:example.org"
      :event/sender "@alice:example.org"
      :event/timestamp "2026-05-16T12:34:57Z"
      :event/text "!status"})
    (extension/handle-broker-event!
     deps
     pi
     ctx
     relay-state
     {:type "matrix.reaction"
      :room/id "!room:example.org"
      :event/id "$reaction:example.org"
      :event/sender "@alice:example.org"
      :event/timestamp "2026-05-16T12:34:58Z"
      :event/reacts-to-id "$ignored:example.org"
      :reaction/key "👍"})
    (is (= [] @sent*))
    (is (= 1 (count @acks*)))
    (is (str/includes? (:message (first @acks*)) "pi-matrix-relay status"))))

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
                                                      :room/id "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}
        event {:type "matrix.message"
               :room/id "!room:example.org"
               :event/id "$event:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "please check status"}]
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
                                                      :room/id "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}
        event {:type "matrix.reaction"
               :room/id "!room:example.org"
               :event/id "$reaction:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/reacts-to-id "$event:example.org"
                       :reaction/key "👍"}]
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
        relay-state {:project-config {:rooms [{:room/name "Shared"
                                               :room/id "!first:example.org"
                                               :mode "all"}
                                              {:room/name "Shared"
                                               :room/id "!second:example.org"
                                               :mode "all"}]}
                     :global-operators #{"@alice:example.org"}
                     :bot-user-id "@bot:example.org"}
        event {:type "matrix.message"
               :room/id "!first:example.org"
               :event/id "$event:example.org"
                       :event/sender "@alice:example.org"
                       :event/timestamp "2026-05-16T12:34:56+02:00"
                       :event/text "hello from ambiguous room"}]
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
                                                      :room/id "!room:example.org"
                                                      :mode "all"}}}
                     :global-operators #{"@alice:example.org"}}
        event {:type "matrix.message"
               :room/id "!room:example.org"
               :event/id "$event:example.org"
                       :event/sender "@mallory:example.org"
                       :event/timestamp "2026-05-16T12:34:56Z"
                       :event/text "please check status"}]
    (extension/handle-broker-event! {} pi ctx relay-state event)
    (is (= [] @sent*))))
