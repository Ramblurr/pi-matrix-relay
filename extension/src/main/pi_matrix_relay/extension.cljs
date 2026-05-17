(ns pi-matrix-relay.extension
  (:require [clojure.string :as str]
            [pi-matrix-relay.broker-client :as broker-client]
            [pi-matrix-relay.commands :as commands]
            [pi-matrix-relay.config :as config]
            [pi-matrix-relay.setup :as setup]))

(defn greeting [name]
  (str "Hello, " name ", from ClojureScript!"))

(def default-deps
  {:health! broker-client/health!
   :register-client! broker-client/register-client!
   :update-subscriptions! broker-client/update-subscriptions!
   :heartbeat! broker-client/heartbeat!
   :unregister-client! broker-client/unregister-client!
   :acquire-slot! broker-client/acquire-slot!
   :release-slot! broker-client/release-slot!
   :list-slots! broker-client/list-slots!
   :list-rooms! broker-client/list-rooms!
   :open-event-stream! broker-client/open-event-stream!
   :resolve-room! broker-client/resolve-room!
   :send-message! broker-client/send-message!
   :send-reaction! broker-client/send-reaction!
   :set-interval! js/setInterval
   :clear-interval! js/clearInterval
   :read-project-config! config/read-project-config!
   :write-project-config! config/write-project-config!
   :run-setup! setup/run-setup!})

(defn- promise
  [value]
  (js/Promise.resolve value))

(defn- ctx-cwd
  [^js ctx]
  (or (.-cwd ctx) "."))

(defn- notify!
  ([ctx message]
   (notify! ctx message "info"))
  ([^js ctx message level]
   (when-let [ui (.-ui ctx)]
     (.notify ui message level))))

(defn- notify-error!
  [ctx message]
  (notify! ctx message "error"))

(defn- set-status!
  [^js ctx status]
  (when-let [ui (.-ui ctx)]
    (when-let [set-status (.-setStatus ui)]
      (set-status "pi-matrix-relay" (if (nil? status) js/undefined status)))))

(defn- rooms-map
  [project-config]
  (let [rooms (:rooms project-config)]
    (cond
      (map? rooms) rooms
      (sequential? rooms) (into {} (keep (fn [binding]
                                           (when-let [alias (:alias binding)]
                                             [alias binding])))
                                  rooms)
      :else {})))

(defn- room-bindings
  [project-config]
  (let [rooms (:rooms project-config)]
    (cond
      (map? rooms) (vals rooms)
      (sequential? rooms) rooms
      :else [])))

(defn- project-room-ids
  [project-config]
  (->> (room-bindings project-config)
       (keep :roomId)
       distinct
       vec))

(defn- effective-project-id
  [cwd project-config]
  (or (get-in project-config [:project :id])
      (config/project-id cwd)))

(defn- project-summary
  [cwd project-config]
  (let [project-id (effective-project-id cwd project-config)]
    {:root cwd
     :id project-id
     :displayName (or (get-in project-config [:project :displayName]) project-id)}))

(defn- now-iso []
  (.toISOString (js/Date.)))

(defn- error-summary
  [err]
  (cond-> {:message (or (.-message err) (str err))}
    (.-data err) (assoc :data (js->clj (.-data err) :keywordize-keys true))
    (.-code err) (assoc :code (.-code err))))

(defn- record-diagnostic!
  ([diagnostics* type]
   (record-diagnostic! diagnostics* type {}))
  ([diagnostics* type details]
   (when diagnostics*
     (let [event (merge {:at (now-iso)
                         :type (name type)}
                        details)]
       (swap! diagnostics* update :events
              (fnil (fn [events]
                      (vec (take-last 50 (conj (vec events) event))))
                    []))))
   nil))

(defn- start-banner
  [cwd project slot]
  (str "pi-matrix-relay session started\n"
       "project: " (:id project) "\n"
       "slot: " (:slot slot) "\n"
       "room: " (:roomName slot) "\n"
       "path: " cwd "\n"
       "time: " (now-iso)))

(defn- end-banner
  [relay-state]
  (str "pi-matrix-relay session ended\n"
       "project: " (get-in relay-state [:project :id]) "\n"
       "slot: " (:slot relay-state) "\n"
       "time: " (now-iso)))

(defn- status-text
  [project-config slot]
  (let [room-aliases (->> (room-bindings project-config)
                          (map :alias)
                          (remove str/blank?)
                          distinct
                          vec)
        base (str "matrix: slot " (:slot slot) " " (:roomName slot))]
    (if (seq room-aliases)
      (str base "; rooms: " (str/join ", " room-aliases))
      base)))

(defn- binding-for-room-id
  [project-config room-id]
  (some (fn [binding]
          (when (= room-id (:roomId binding))
            binding))
        (room-bindings project-config)))

(defn- slot-binding-for-room-id
  [relay-state room-id]
  (when (and (:room-id relay-state)
             (= room-id (:room-id relay-state)))
    {:alias (:room-name relay-state)
     :name (:room-name relay-state)
     :roomId (:room-id relay-state)
     :roomClass "slot"
     :mode "all"
     :busy config/default-busy-behavior
     :autoReply true
     :slot (:slot relay-state)}))

(defn- binding-for-relay-room
  [relay-state room-id]
  (or (slot-binding-for-room-id relay-state room-id)
      (binding-for-room-id (:project-config relay-state) room-id)))

(defn- authorized-sender?
  [{:keys [project-config global-operators]} sender]
  (let [global-operators (set global-operators)
        project-users (set (:allowedUsers project-config))]
    (boolean (or (contains? global-operators sender)
                 (contains? project-users sender)))))

(defn- short-time
  [timestamp]
  (if-let [[_ time zone] (and (string? timestamp)
                              (re-find #"T(\d\d:\d\d:\d\d)(?:\.\d+)?(Z|[+-]\d\d:\d\d)?" timestamp))]
    (str time (or zone ""))
    "??:??:??"))

(defn- localpart
  [mxid]
  (when-let [[_ user] (and mxid (re-matches #"^@([^:]+):.+$" mxid))]
    user))

(defn- mentions-bot?
  [text bot-user-id]
  (let [text (str text)
        user (localpart bot-user-id)]
    (boolean (or (and (seq bot-user-id) (str/includes? text bot-user-id))
                 (and (seq user) (re-find (js/RegExp. (str "(^|[^A-Za-z0-9_-])@?" user "([^A-Za-z0-9_-]|$)") "i") text))))))

(defn- message-allowed-by-mode?
  [binding text bot-user-id]
  (case (or (:mode binding) config/default-room-mode)
    "all" true
    "mentions" (mentions-bot? text bot-user-id)
    "commands-only" (str/starts-with? (str/trim (str text)) "/")
    false))

(defn- room-label
  ([binding]
   (room-label binding nil))
  ([binding room]
   (or (:alias binding)
       (:name binding)
       (:canonicalAlias binding)
       (:name room)
       (:canonicalAlias room)
       (:roomId binding)
       (:roomId room))))

(defn- ambiguous-room-label?
  [project-config binding room]
  (let [label (room-label binding room)]
    (< 1 (count (filter #(= label (room-label %))
                        (room-bindings project-config))))))

(defn- metadata-lines
  [base-lines include-room-id? room-id]
  (cond-> base-lines
    include-room-id? (conj (str "roomId: " room-id))))

(defn- matrix-message-prompt
  [project-config binding {:keys [room event]}]
  (let [label (room-label binding room)
        sender (or (:sender event) "unknown sender")
        timestamp (short-time (:timestamp event))
        text (or (:text event) "")
        include-room-id? (ambiguous-room-label? project-config binding room)
        metadata (metadata-lines [(str "eventId: " (:eventId event))]
                                 include-room-id?
                                 (get room :roomId))]
    (str "Matrix " label " from " sender " at " timestamp "\n"
         text "\n\n"
         "Matrix metadata:\n"
         (str/join "\n" (cond-> metadata
                           (:replyToEventId event)
                           (conj (str "replyToEventId: " (:replyToEventId event))))))))

(defn- matrix-reaction-prompt
  [project-config binding {:keys [room event]}]
  (let [label (room-label binding room)
        sender (or (:sender event) "unknown sender")
        timestamp (short-time (:timestamp event))
        include-room-id? (ambiguous-room-label? project-config binding room)
        metadata (metadata-lines [(str "eventId: " (:eventId event))
                                  (str "reactsToEventId: " (:reactsToEventId event))]
                                 include-room-id?
                                 (get room :roomId))]
    (str "Matrix reaction in " label " from " sender " at " timestamp "\n"
         "reacted " (:key event) " to event " (:reactsToEventId event) "\n\n"
         "Matrix metadata:\n"
         (str/join "\n" metadata))))

(defn- idle?
  [^js ctx]
  (if-let [is-idle (.-isIdle ctx)]
    (boolean (is-idle))
    true))

(defn- deliver-user-message!
  [^js pi ^js ctx binding prompt]
  (if (idle? ctx)
    (do
      (.sendUserMessage pi prompt)
      true)
    (case (or (:busy binding) config/default-busy-behavior)
      "steer" (do
                 (.sendUserMessage pi prompt #js {:deliverAs "steer"})
                 true)
      "reject" (do
                  (notify! ctx "Matrix message ignored because the agent is busy." "warning")
                  false)
      (do
        (.sendUserMessage pi prompt #js {:deliverAs "followUp"})
        true))))

(defn handle-broker-event!
  [_deps pi ctx relay-state event]
  (case (:type event)
    "matrix.message"
    (let [room-id (get-in event [:room :roomId])
          message (:event event)
          binding (binding-for-relay-room relay-state room-id)]
      (when (and binding
                 (not (:senderIsBot message))
                 (authorized-sender? relay-state (:sender message))
                 (message-allowed-by-mode? binding (:text message) (:bot-user-id relay-state)))
        (let [delivered? (deliver-user-message! pi ctx binding (matrix-message-prompt (:project-config relay-state) binding event))]
          (when (and delivered? (:autoReply binding) (:pending-auto-replies* relay-state))
            (swap! (:pending-auto-replies* relay-state) conj {:room-id room-id
                                                              :event-id (:eventId message)})))))

    "matrix.reaction"
    (let [room-id (get-in event [:room :roomId])
          reaction (:event event)
          binding (binding-for-relay-room relay-state room-id)]
      (when (and binding
                 (not (:senderIsBot reaction))
                 (authorized-sender? relay-state (:sender reaction)))
        (deliver-user-message! pi ctx binding (matrix-reaction-prompt (:project-config relay-state) binding event))))

    "broker.notice"
    (notify! ctx (:message event) (or (:level event) "info"))

    nil)
  nil)

(defn- start-heartbeat!
  [{:keys [heartbeat! set-interval! diagnostics*]} ctx client-id heartbeat-seconds]
  (when (and heartbeat! set-interval!)
    (set-interval!
     (fn []
       (-> (promise (heartbeat! client-id))
           (.catch (fn [err]
                     (record-diagnostic! diagnostics* :heartbeat-error (error-summary err))
                     (notify! ctx (str "Matrix relay heartbeat failed: " (.-message err)) "warning")))))
     (* 1000 (or heartbeat-seconds 30)))))

(defn start-relay!
  [{:keys [read-project-config! health! register-client! acquire-slot!
           update-subscriptions! send-message! open-event-stream! diagnostics*] :as deps}
   pi ctx]
  (let [cwd (ctx-cwd ctx)
        project-config (read-project-config! cwd)
        room-ids (project-room-ids project-config)
        project (project-summary cwd project-config)]
    (record-diagnostic! diagnostics* :start {:cwd cwd
                                             :projectId (:id project)})
    (-> (promise (health!))
        (.then
         (fn [health]
           (record-diagnostic! diagnostics* :health-ok {:matrix (select-keys (:matrix health) [:connected :userId])})
           (let [request {:clientInstanceId (str "matrix-relay-" cwd)
                          :protocolVersion 1
                          :project (select-keys project [:root :id])
                          :subscriptions {:rooms room-ids}}]
             (-> (promise (register-client! request))
                 (.then
                  (fn [registration]
                    (let [client-id (:clientId registration)
                          global-operators (vec (:globalOperators registration))]
                      (record-diagnostic! diagnostics* :client-registered {:clientId client-id})
                      (-> (promise (acquire-slot! client-id
                                                   (select-keys project [:id :displayName])
                                                   global-operators))
                          (.then
                           (fn [slot]
                             (record-diagnostic! diagnostics* :slot-acquired {:slot (:slot slot)
                                                                              :roomId (:roomId slot)
                                                                              :roomName (:roomName slot)})
                             (let [slot-room-id (:roomId slot)
                                   subscriptions (vec (distinct (conj room-ids slot-room-id)))
                                   banner (start-banner cwd project slot)]
                               (-> (promise (update-subscriptions! client-id subscriptions))
                                   (.then
                                    (fn [_]
                                      (record-diagnostic! diagnostics* :subscriptions-updated {:clientId client-id
                                                                                               :rooms subscriptions})
                                      (-> (promise (send-message! slot-room-id banner {:clientId client-id}))
                                          (.then
                                           (fn [_]
                                             (record-diagnostic! diagnostics* :start-banner-sent {:roomId slot-room-id})
                                             (let [heartbeat-id (start-heartbeat! deps ctx client-id (:heartbeatSeconds registration))
                                                   relay-state {:client-id client-id
                                                                :project-config project-config
                                                                :project project
                                                                :global-operators (set global-operators)
                                                                :bot-user-id (get-in health [:matrix :userId])
                                                                :slot (:slot slot)
                                                                :room-id slot-room-id
                                                                :room-name (:roomName slot)
                                                                :pending-auto-replies* (atom [])
                                                                :last-start-banner banner
                                                                :heartbeat-id heartbeat-id}
                                                   stream (open-event-stream!
                                                           {:on-error (fn [err]
                                                                        (record-diagnostic! diagnostics* :event-stream-error (error-summary err))
                                                                        (notify! ctx (str "Matrix relay event stream failed: " (.-message err)) "warning"))}
                                                           client-id
                                                           #(handle-broker-event! deps pi ctx relay-state %))]
                                               (record-diagnostic! diagnostics* :event-stream-opened {:clientId client-id})
                                               (set-status! ctx (status-text project-config slot))
                                               (assoc relay-state :stream stream))))))))))))))))))))))

(defn- ignore-errors
  [thunk]
  (try
    (-> (promise (thunk))
        (.catch (fn [_] nil)))
    (catch js/Error _
      (promise nil))))

(defn stop-relay!
  ([relay-state]
   (stop-relay! {} nil relay-state))
  ([{:keys [clear-interval! send-message! release-slot! unregister-client!]} ctx relay-state]
   (when-let [stream (:stream relay-state)]
     (when-let [close (.-close stream)]
       (close)))
   (when-let [heartbeat-id (:heartbeat-id relay-state)]
     (when clear-interval!
       (clear-interval! heartbeat-id)))
   (-> (ignore-errors
        #(when (and send-message! (:room-id relay-state) (:client-id relay-state))
           (send-message! (:room-id relay-state)
                          (end-banner relay-state)
                          {:clientId (:client-id relay-state)})))
       (.then (fn [_]
                (ignore-errors
                 #(when (and release-slot! (:client-id relay-state) (:room-id relay-state) (:slot relay-state))
                    (release-slot! (:client-id relay-state) (:room-id relay-state) (:slot relay-state))))))
       (.then (fn [_]
                (ignore-errors
                 #(when (and unregister-client! (:client-id relay-state))
                    (unregister-client! (:client-id relay-state) "shutdown")))))
       (.then (fn [_]
                (when ctx
                  (set-status! ctx nil))
                nil)))))

(defn- message-role
  [message]
  (or (:role message)
      (when (map? message) (get message "role"))))

(defn- message-stop-reason
  [message]
  (or (:stopReason message)
      (:stop-reason message)
      (when (map? message) (get message "stopReason"))))

(defn- content-text
  [content]
  (cond
    (string? content) content
    (sequential? content) (->> content
                               (keep (fn [part]
                                       (when (= "text" (or (:type part) (get part "type")))
                                         (or (:text part) (get part "text")))))
                               (str/join "\n"))
    :else ""))

(defn- assistant-final-text
  [event]
  (let [assistant (->> (:messages event)
                       (filter #(= "assistant" (message-role %)))
                       last)
        stop-reason (message-stop-reason assistant)
        text (some-> assistant :content content-text str/trim)]
    (when (and assistant
               (not (contains? #{"aborted" "error"} stop-reason))
               (not (str/blank? text)))
      text)))

(defn handle-agent-end!
  [{:keys [send-message!]} relay-state event]
  (let [pending* (:pending-auto-replies* relay-state)
        target (first @pending*)
        text (assistant-final-text event)]
    (if (and send-message! target text)
      (-> (promise (send-message! (:room-id target)
                                  text
                                  {:clientId (:client-id relay-state)
                                   :replyToEventId (:event-id target)}))
          (.then (fn [_]
                   (swap! pending* #(vec (rest %)))
                   nil))
          (.catch (fn [_]
                    (swap! pending* #(vec (rest %)))
                    nil)))
      (promise nil))))

(defn- safe-project-config
  [{:keys [read-project-config!]} cwd]
  (try
    (if read-project-config!
      (or (read-project-config! cwd) {})
      {})
    (catch js/Error err
      {:error (error-summary err)})))

(defn- stream-diagnostics
  [relay-state]
  (when-let [^js stream (:stream relay-state)]
    (when-let [diagnostics (.-diagnostics stream)]
      (js->clj (diagnostics) :keywordize-keys true))))

(defn- relay-snapshot
  [relay-state]
  (if relay-state
    (let [project-config (:project-config relay-state)
          pending* (:pending-auto-replies* relay-state)]
      (cond-> {:running true
               :clientId (:client-id relay-state)
               :project (:project relay-state)
               :projectRooms (project-room-ids project-config)
               :globalOperators (vec (sort (:global-operators relay-state)))
               :botUserId (:bot-user-id relay-state)
               :slot (:slot relay-state)
               :roomId (:room-id relay-state)
               :roomName (:room-name relay-state)
               :statusText (status-text project-config {:slot (:slot relay-state)
                                                         :roomName (:room-name relay-state)})
               :heartbeatActive (boolean (:heartbeat-id relay-state))
               :streamActive (boolean (:stream relay-state))
               :pendingAutoReplies (if pending* (count @pending*) 0)}
        (:last-start-banner relay-state)
        (assoc :lastStartBanner (:last-start-banner relay-state))
        (stream-diagnostics relay-state)
        (assoc :streamDiagnostics (stream-diagnostics relay-state))))
    {:running false}))

(defn- settled
  [thunk]
  (try
    (-> (promise (thunk))
        (.then (fn [value]
                 {:ok true :value value}))
        (.catch (fn [err]
                  {:ok false :error (error-summary err)})))
    (catch js/Error err
      (promise {:ok false :error (error-summary err)}))))

(defn- settled-result
  [result]
  (if (:ok result)
    (:value result)
    {:error (:error result)}))

(defn- broker-summary-line
  [broker]
  (let [matrix (get-in broker [:health :matrix])]
    (cond
      (:error (:health broker)) (str "broker: error " (get-in broker [:health :error :message]))
      (:connected matrix) (str "broker: Matrix connected as " (:userId matrix))
      matrix "broker: Matrix not connected"
      :else "broker: not queried")))

(defn- slots-summary-line
  [broker]
  (if-let [slots (seq (get-in broker [:slots :slots]))]
    (str "slots: " (str/join ", " (map (fn [slot]
                                           (str (:slot slot) " " (:state slot) " " (:roomName slot)))
                                         slots)))
    "slots: none visible"))

(defn- diagnostics-summary
  [{:keys [relay broker]}]
  (str "pi-matrix-relay diagnostics\n"
       (if (:running relay)
         (str "extension: running slot " (:slot relay) " " (:roomName relay) "\n"
              "client: " (:clientId relay) "\n"
              "room: " (:roomId relay) "\n"
              "heartbeat: " (if (:heartbeatActive relay) "active" "inactive") ", stream: "
              (if (:streamActive relay) "active" "inactive") "\n")
         "extension: not running\n")
       (broker-summary-line broker) "\n"
       (slots-summary-line broker)))

(defn execute-matrix-relay-diagnostics!
  [deps params ^js ctx]
  (let [{:keys [health! list-slots! list-rooms! relay-state* diagnostics*]} deps
        cwd (ctx-cwd ctx)
        project-config (safe-project-config deps cwd)
        project (project-summary cwd project-config)
        relay (relay-snapshot (some-> relay-state* deref))
        include-broker? (not= false (:includeBroker params))
        include-rooms? (true? (:includeRooms params))
        health-p (if (and include-broker? health!)
                   (settled #(health!))
                   (promise {:ok false :error {:message "broker health not queried"}}))
        slots-p (if (and include-broker? list-slots!)
                  (settled #(list-slots! (:id project)))
                  (promise {:ok true :value {:projectId (:id project) :slots []}}))
        rooms-p (if (and include-broker? include-rooms? list-rooms!)
                  (settled #(list-rooms!))
                  (promise {:ok true :value nil}))]
    (-> (js/Promise.all (clj->js [health-p slots-p rooms-p]))
        (.then (fn [results]
                 (let [health-result (aget results 0)
                       slots-result (aget results 1)
                       rooms-result (aget results 2)
                       broker (cond-> {:health (settled-result health-result)
                                       :slots (settled-result slots-result)}
                                (some? (settled-result rooms-result))
                                (assoc :rooms (settled-result rooms-result)))
                       details {:cwd cwd
                                :project project
                                :projectConfig {:rooms (vec (room-bindings project-config))
                                                :allowedUsers (vec (:allowedUsers project-config))}
                                :relay relay
                                :diagnostics {:events (vec (:events (if diagnostics* @diagnostics* {})))}
                                :broker broker}]
                   {:content [{:type "text"
                               :text (diagnostics-summary details)}]
                    :details details}))))))

(defn- control-diagnostics
  [deps ctx]
  (execute-matrix-relay-diagnostics! deps {:includeBroker true} ctx))

(defn execute-matrix-relay-control!
  [deps params pi ctx]
  (let [{:keys [relay-state* diagnostics*]} deps
        action (or (:action params) "status")]
    (case action
      "status"
      (control-diagnostics deps ctx)

      "start"
      (if (and relay-state* @relay-state*)
        (control-diagnostics deps ctx)
        (-> (start-relay! deps pi ctx)
            (.then (fn [relay-state]
                     (when relay-state*
                       (reset! relay-state* relay-state))
                     (record-diagnostic! diagnostics* :control-started {:clientId (:client-id relay-state)})
                     relay-state))
            (.then (fn [_]
                     (control-diagnostics deps ctx)))
            (.catch (fn [err]
                      (record-diagnostic! diagnostics* :control-start-error (error-summary err))
                      (js/Promise.reject err)))))

      "stop"
      (if-let [relay-state (and relay-state* @relay-state*)]
        (-> (stop-relay! deps ctx relay-state)
            (.then (fn [_]
                     (when relay-state*
                       (reset! relay-state* nil))
                     (record-diagnostic! diagnostics* :control-stopped)
                     nil))
            (.then (fn [_]
                     (control-diagnostics deps ctx))))
        (control-diagnostics deps ctx))

      "restart"
      (let [stop-promise (if-let [relay-state (and relay-state* @relay-state*)]
                           (-> (stop-relay! deps ctx relay-state)
                               (.then (fn [_]
                                        (when relay-state*
                                          (reset! relay-state* nil))
                                        (record-diagnostic! diagnostics* :control-stopped)
                                        nil)))
                           (promise nil))]
        (-> stop-promise
            (.then (fn [_]
                     (start-relay! deps pi ctx)))
            (.then (fn [relay-state]
                     (when relay-state*
                       (reset! relay-state* relay-state))
                     (record-diagnostic! diagnostics* :control-started {:clientId (:client-id relay-state)})
                     relay-state))
            (.then (fn [_]
                     (control-diagnostics deps ctx)))
            (.catch (fn [err]
                      (record-diagnostic! diagnostics* :control-restart-error (error-summary err))
                      (js/Promise.reject err)))))

      (js/Promise.reject (js/Error. (str "Unknown matrix relay control action: " action))))))

(defn- handle-help!
  [ctx]
  (notify! ctx (str "Matrix relay commands: setup, status, room bind <room> [alias], "
                    "send <alias-or-room-id> <message>"))
  (promise nil))

(defn- handle-setup!
  [{:keys [run-setup!] :as deps} ^js ctx]
  (let [ui (.-ui ctx)]
    (run-setup!
     (merge deps
            {:input! (fn [label placeholder]
                       (.input ui label placeholder))
             :editor! (fn [label initial]
                        (.editor ui label initial))
             :confirm! (fn [title message]
                         (.confirm ui title message))
             :notify! (fn [message level]
                        (.notify ui message level))
             :set-status! (fn [status]
                            (.setStatus ui "pi-matrix-relay" status))}))))

(defn- handle-status!
  [{:keys [health!]} ctx]
  (-> (promise (health!))
      (.then (fn [health]
               (let [matrix (:matrix health)]
                 (notify! ctx
                          (if (:connected matrix)
                            (str "Matrix connected as " (:userId matrix))
                            "Matrix broker is not connected")
                          (if (:connected matrix) "info" "warning")))))))

(defn- handle-room-bind!
  [{:keys [resolve-room! read-project-config! write-project-config!]} {:keys [room alias]} ctx]
  (let [cwd (ctx-cwd ctx)]
    (-> (promise (resolve-room! room))
        (.then (fn [room-result]
                 (let [old-config (read-project-config! cwd)
                       new-config (config/bind-room old-config room-result alias cwd)
                       binding (config/room-binding room-result alias cwd)]
                   (write-project-config! cwd new-config)
                   (notify! ctx (str "Bound " (:alias binding) " to " (:roomId binding)))))))))

(defn- handle-send!
  [{:keys [read-project-config! send-message!]} {:keys [target message]} ctx]
  (let [cwd (ctx-cwd ctx)
        project-config (read-project-config! cwd)]
    (if-let [binding (config/resolve-target project-config target)]
      (-> (promise (send-message! (:roomId binding) message))
          (.then (fn [result]
                   (notify! ctx (str "Sent Matrix message " (:eventId result))))))
      (do
        (notify-error! ctx (str "No Matrix room binding for target " target))
        (promise nil)))))

(defn handle-command!
  ([args ctx]
   (handle-command! default-deps args ctx))
  ([deps args ctx]
   (let [{:keys [op message] :as command} (commands/parse args)]
     (case op
       :help (handle-help! ctx)
       :setup (handle-setup! deps ctx)
       :status (handle-status! deps ctx)
       :room-bind (handle-room-bind! deps command ctx)
       :send (handle-send! deps command ctx)
       :error (do
                (notify-error! ctx message)
                (promise nil))))))

(defn- relay-client-id
  [deps]
  (some-> (:relay-state* deps) deref :client-id))

(def diagnostic-targets
  #{"matrix-relay:diagnostics" "__matrix_relay_diagnostics__"})

(def control-targets
  #{"matrix-relay:control" "__matrix_relay_control__"})

(defn- control-action
  [message]
  (let [action (str/trim (str message))]
    (if (seq action) action "status")))

(defn- matrix-room-id?
  [target]
  (and (string? target)
       (str/starts-with? target "!")))

(defn- resolve-send-target
  [project-config target]
  (or (config/resolve-target project-config target)
      (when (matrix-room-id? target)
        {:alias target
         :roomId target})))

(defn execute-send-matrix-message!
  [deps params ^js ctx]
  (let [{:keys [read-project-config! send-message! pi]} deps
        cwd (ctx-cwd ctx)
        target (:target params)
        message (:message params)
        reply-to-event-id (:replyToEventId params)
        client-id (relay-client-id deps)]
    (cond
      (contains? diagnostic-targets target)
      (execute-matrix-relay-diagnostics! deps {:includeBroker true
                                               :includeRooms true}
                                         ctx)

      (contains? control-targets target)
      (execute-matrix-relay-control! deps {:action (control-action message)} pi ctx)

      :else
      (let [project-config (read-project-config! cwd)]
        (if-let [binding (resolve-send-target project-config target)]
          (-> (promise (send-message! (:roomId binding)
                                      message
                                      (cond-> {}
                                        client-id (assoc :clientId client-id)
                                        reply-to-event-id (assoc :replyToEventId reply-to-event-id))))
              (.then (fn [result]
                       {:content [{:type "text"
                                   :text (str "Sent Matrix message " (:eventId result)
                                              " to " (:roomId binding))}]
                        :details (cond-> {:roomId (:roomId binding)
                                          :eventId (:eventId result)
                                          :target target}
                                   reply-to-event-id (assoc :replyToEventId reply-to-event-id))})))
          (js/Promise.reject (js/Error. (str "No Matrix room binding for target " target))))))))

(defn execute-send-matrix-reaction!
  [deps params ^js ctx]
  (let [{:keys [read-project-config! send-reaction!]} deps
        cwd (ctx-cwd ctx)
        target (:target params)
        event-id (:eventId params)
        key (:key params)
        client-id (relay-client-id deps)
        project-config (read-project-config! cwd)]
    (if-let [binding (resolve-send-target project-config target)]
      (-> (promise (send-reaction! (:roomId binding)
                                    event-id
                                    key
                                    (cond-> {}
                                      client-id (assoc :clientId client-id))))
          (.then (fn [result]
                   {:content [{:type "text"
                               :text (str "Sent Matrix reaction " key " to " event-id
                                          " in " (:roomId binding))}]
                    :details {:roomId (:roomId binding)
                              :eventId (:eventId result)
                              :reactsToEventId event-id
                              :target target
                              :key key}})))
      (js/Promise.reject (js/Error. (str "No Matrix room binding for target " target))))))

(def send-matrix-message-parameters
  #js {:type "object"
       :additionalProperties false
       :required #js ["target" "message"]
       :properties #js {:target #js {:type "string"
                                     :description "Bound local alias or Matrix room id to send to"}
                        :message #js {:type "string"
                                      :description "Plain text Matrix message body"}
                        :replyToEventId #js {:type "string"
                                             :description "Optional Matrix event id to reply to using Matrix-native reply metadata"}}})

(defn register-send-tool!
  [^js pi deps]
  (.registerTool pi
    #js {:name "send_matrix_message"
         :label "Send Matrix Message"
         :description "Send a plain text Matrix message through the local pi-matrix-relay broker."
         :promptSnippet "Send a Matrix message to a bound project room alias."
         :promptGuidelines #js ["Use send_matrix_message only when the user explicitly asks to send a Matrix message."
                                "Use replyToEventId when replying to a Matrix message that included an eventId in the prompt metadata."]
         :parameters send-matrix-message-parameters
         :execute (fn [_tool-call-id params _signal _on-update ctx]
                    (-> (execute-send-matrix-message! deps (js->clj params :keywordize-keys true) ctx)
                        (.then clj->js)))}))

(def send-matrix-reaction-parameters
  #js {:type "object"
       :additionalProperties false
       :required #js ["target" "eventId" "key"]
       :properties #js {:target #js {:type "string"
                                     :description "Bound local alias or Matrix room id containing the event"}
                        :eventId #js {:type "string"
                                      :description "Matrix event id to react to"}
                        :key #js {:type "string"
                                  :description "Reaction key, for example 👍"}}})

(defn register-reaction-tool!
  [^js pi deps]
  (.registerTool pi
    #js {:name "send_matrix_reaction"
         :label "Send Matrix Reaction"
         :description "Send a Matrix reaction through the local pi-matrix-relay broker."
         :promptSnippet "React to a Matrix event in a bound project room."
         :promptGuidelines #js ["Use send_matrix_reaction only when the user explicitly asks to react to a Matrix message."]
         :parameters send-matrix-reaction-parameters
         :execute (fn [_tool-call-id params _signal _on-update ctx]
                    (-> (execute-send-matrix-reaction! deps (js->clj params :keywordize-keys true) ctx)
                        (.then clj->js)))}))

(def matrix-relay-diagnostics-parameters
  #js {:type "object"
       :additionalProperties false
       :properties #js {:includeBroker #js {:type "boolean"
                                            :description "Also query broker health and slot state. Defaults to true."}
                        :includeRooms #js {:type "boolean"
                                           :description "Also list Matrix rooms known to the broker. Defaults to false because this can be verbose."}}})

(defn register-diagnostics-tool!
  [^js pi deps]
  (.registerTool pi
    #js {:name "matrix_relay_diagnostics"
         :label "Matrix Relay Diagnostics"
         :description "Inspect this Pi process' pi-matrix-relay extension state and optionally compare it with broker state."
         :promptSnippet "Use matrix_relay_diagnostics to debug whether this Pi session is listening to Matrix rooms, which slot it has, and what the broker sees."
         :promptGuidelines #js ["Use matrix_relay_diagnostics instead of /mr status when debugging Matrix relay state from an agent."
                                "Do not rely on slash commands for relay troubleshooting; this tool returns the running extension internals."]
         :parameters matrix-relay-diagnostics-parameters
         :execute (fn [_tool-call-id params _signal _on-update ctx]
                    (-> (execute-matrix-relay-diagnostics! deps (js->clj params :keywordize-keys true) ctx)
                        (.then clj->js)))}))

(def matrix-relay-control-parameters
  #js {:type "object"
       :additionalProperties false
       :required #js ["action"]
       :properties #js {:action #js {:type "string"
                                     :enum #js ["status" "start" "stop" "restart"]
                                     :description "Relay lifecycle action for this Pi process."}}})

(defn register-control-tool!
  [^js pi deps]
  (.registerTool pi
    #js {:name "matrix_relay_control"
         :label "Matrix Relay Control"
         :description "Start, stop, restart, or inspect this Pi process' Matrix relay without using slash commands."
         :promptSnippet "Use matrix_relay_control when the relay needs to be started or restarted for this Pi session during debugging."
         :promptGuidelines #js ["Use status or matrix_relay_diagnostics before mutating relay state."
                                "Use restart only when the current relay state is stale or failed and the user is debugging the relay."]
         :parameters matrix-relay-control-parameters
         :execute (fn [_tool-call-id params _signal _on-update ctx]
                    (-> (execute-matrix-relay-control! deps (js->clj params :keywordize-keys true) pi ctx)
                        (.then clj->js)))}))

(def relay-tool-names
  ["send_matrix_message"
   "send_matrix_reaction"
   "matrix_relay_diagnostics"
   "matrix_relay_control"])

(defn activate-relay-tools!
  [^js pi]
  (when (and (.-getActiveTools pi)
             (.-setActiveTools pi))
    (let [active-tools (set (js->clj (.getActiveTools pi)))
          next-tools (vec (distinct (concat active-tools relay-tool-names)))]
      (.setActiveTools pi (clj->js next-tools)))))

(defn hello-handler [args ^js ctx]
  (let [target (if (and (string? args)
                        (not= "" (.trim args)))
                 (.trim args)
                 "world")
        ^js ui (.-ui ctx)]
    (.notify ui (greeting target) "info")))

(defn init
  ([^js pi]
   (init pi default-deps))
  ([^js pi deps]
   (let [relay-state* (atom nil)
         diagnostics* (atom {})
         deps (merge default-deps deps {:relay-state* relay-state*
                                        :diagnostics* diagnostics*
                                        :pi pi})]
     (doseq [command-name ["matrix-relay" "mr"]]
       (.registerCommand pi command-name
         #js {:description "Control the Pi Matrix relay"
              :handler (fn [args ctx]
                         (handle-command! deps args ctx))}))
     (register-send-tool! pi deps)
     (register-reaction-tool! pi deps)
     (register-diagnostics-tool! pi deps)
     (register-control-tool! pi deps)
     (when-let [on (.-on pi)]
       (on "session_start"
           (fn [_event ctx]
             (activate-relay-tools! pi)
             (-> (start-relay! deps pi ctx)
                 (.then #(reset! relay-state* %))
                 (.catch (fn [err]
                           (record-diagnostic! diagnostics* :start-error (error-summary err))
                           (notify! ctx (str "Matrix relay receive disabled: " (.-message err)) "warning"))))))
       (on "agent_end"
           (fn [event _ctx]
             (when-let [relay-state @relay-state*]
               (handle-agent-end! deps relay-state (js->clj event :keywordize-keys true)))))
       (on "session_shutdown"
           (fn [_event ctx]
             (-> (stop-relay! deps ctx @relay-state*)
                 (.finally (fn []
                             (reset! relay-state* nil))))))))))