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
   :open-event-stream! broker-client/open-event-stream!
   :resolve-room! broker-client/resolve-room!
   :send-message! broker-client/send-message!
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
      (set-status "pi-matrix-relay" status))))

(defn- rooms-map
  [project-config]
  (let [rooms (:rooms project-config)]
    (cond
      (map? rooms) rooms
      (sequential? rooms) (into {} (map (juxt :alias identity)) rooms)
      :else {})))

(defn- room-bindings
  [project-config]
  (vals (rooms-map project-config)))

(defn- project-room-ids
  [project-config]
  (->> (room-bindings project-config)
       (keep :roomId)
       distinct
       vec))

(defn- binding-for-room-id
  [project-config room-id]
  (some (fn [binding]
          (when (= room-id (:roomId binding))
            binding))
        (room-bindings project-config)))

(defn- authorized-sender?
  [{:keys [project-config global-operators]} sender]
  (let [global-operators (set global-operators)
        project-users (set (:allowedUsers project-config))]
    (boolean (or (contains? global-operators sender)
                 (contains? project-users sender)))))

(defn- short-time
  [timestamp]
  (if (and (string? timestamp) (<= 16 (count timestamp)))
    (subs timestamp 11 16)
    "??:??"))

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

(defn- matrix-message-prompt
  [binding {:keys [room event]}]
  (let [alias (or (:alias binding) (:name binding) (get room :name) (get room :roomId))
        sender (or (:sender event) "unknown sender")
        timestamp (short-time (:timestamp event))
        text (or (:text event) "")]
    (str "Matrix " alias " from " sender " at " timestamp "\n"
         text "\n\n"
         "Matrix metadata:\n"
         "roomId: " (get room :roomId) "\n"
         "eventId: " (:eventId event)
         (when-let [reply-to (:replyToEventId event)]
           (str "\nreplyToEventId: " reply-to)))))

(defn- idle?
  [^js ctx]
  (if-let [is-idle (.-isIdle ctx)]
    (boolean (is-idle))
    true))

(defn- deliver-user-message!
  [^js pi ^js ctx binding prompt]
  (if (idle? ctx)
    (.sendUserMessage pi prompt)
    (case (or (:busy binding) config/default-busy-behavior)
      "steer" (.sendUserMessage pi prompt #js {:deliverAs "steer"})
      "reject" (notify! ctx "Matrix message ignored because the agent is busy." "warning")
      (.sendUserMessage pi prompt #js {:deliverAs "followUp"}))))

(defn handle-broker-event!
  [_deps pi ctx relay-state event]
  (case (:type event)
    "matrix.message"
    (let [room-id (get-in event [:room :roomId])
          message (:event event)
          binding (binding-for-room-id (:project-config relay-state) room-id)]
      (when (and binding
                 (not (:senderIsBot message))
                 (authorized-sender? relay-state (:sender message))
                 (message-allowed-by-mode? binding (:text message) (:bot-user-id relay-state)))
        (deliver-user-message! pi ctx binding (matrix-message-prompt binding event))))

    "broker.notice"
    (notify! ctx (:message event) (or (:level event) "info"))

    nil)
  nil)

(defn start-relay!
  [{:keys [read-project-config! health! register-client! open-event-stream!] :as deps} pi ctx]
  (let [cwd (ctx-cwd ctx)
        project-config (read-project-config! cwd)
        room-ids (project-room-ids project-config)]
    (if (empty? room-ids)
      (do
        (set-status! ctx "matrix: no rooms")
        (promise nil))
      (-> (promise (health!))
          (.then
           (fn [health]
             (let [request {:clientInstanceId (str "matrix-relay-" cwd)
                            :protocolVersion 1
                            :project {:root cwd
                                      :id (config/project-id cwd)}
                            :subscriptions {:rooms room-ids}}]
               (-> (promise (register-client! request))
                   (.then
                    (fn [registration]
                      (let [relay-state {:client-id (:clientId registration)
                                         :project-config project-config
                                         :global-operators (set (:globalOperators registration))
                                         :bot-user-id (get-in health [:matrix :userId])}
                            stream (open-event-stream!
                                    (:clientId registration)
                                    #(handle-broker-event! deps pi ctx relay-state %))]
                        (set-status! ctx (str "matrix: listening to "
                                              (str/join ", " (map :alias (room-bindings project-config)))))
                        (assoc relay-state :stream stream))))))))))))

(defn stop-relay!
  [relay-state]
  (when-let [stream (:stream relay-state)]
    (when-let [close (.-close stream)]
      (close)))
  nil)

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

(defn execute-send-matrix-message!
  [deps params ^js ctx]
  (let [{:keys [read-project-config! send-message!]} deps
        cwd (ctx-cwd ctx)
        target (:target params)
        message (:message params)
        project-config (read-project-config! cwd)]
    (if-let [binding (config/resolve-target project-config target)]
      (-> (promise (send-message! (:roomId binding) message))
          (.then (fn [result]
                   {:content [{:type "text"
                               :text (str "Sent Matrix message " (:eventId result)
                                          " to " (:roomId binding))}]
                    :details {:roomId (:roomId binding)
                              :eventId (:eventId result)
                              :target target}})))
      (js/Promise.reject (js/Error. (str "No Matrix room binding for target " target))))))

(def send-matrix-message-parameters
  #js {:type "object"
       :additionalProperties false
       :required #js ["target" "message"]
       :properties #js {:target #js {:type "string"
                                     :description "Bound local alias or Matrix room id to send to"}
                        :message #js {:type "string"
                                      :description "Plain text Matrix message body"}}})

(defn register-send-tool!
  [^js pi deps]
  (.registerTool pi
    #js {:name "send_matrix_message"
         :label "Send Matrix Message"
         :description "Send a plain text Matrix message through the local pi-matrix-relay broker."
         :promptSnippet "Send a Matrix message to a bound project room alias."
         :promptGuidelines #js ["Use send_matrix_message only when the user explicitly asks to send a Matrix message."]
         :parameters send-matrix-message-parameters
         :execute (fn [_tool-call-id params _signal _on-update ctx]
                    (-> (execute-send-matrix-message! deps (js->clj params :keywordize-keys true) ctx)
                        (.then clj->js)))}))

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
   (let [deps (merge default-deps deps)
         relay-state* (atom nil)]
     (doseq [command-name ["matrix-relay" "mr"]]
       (.registerCommand pi command-name
         #js {:description "Control the Pi Matrix relay"
              :handler (fn [args ctx]
                         (handle-command! deps args ctx))}))
     (register-send-tool! pi deps)
     (when-let [on (.-on pi)]
       (on "session_start"
           (fn [_event ctx]
             (-> (start-relay! deps pi ctx)
                 (.then #(reset! relay-state* %))
                 (.catch (fn [err]
                           (notify! ctx (str "Matrix relay receive disabled: " (.-message err)) "warning"))))))
       (on "session_shutdown"
           (fn [_event _ctx]
             (stop-relay! @relay-state*)
             (reset! relay-state* nil)))))))