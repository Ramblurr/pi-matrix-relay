(ns pi-matrix-relay.extension
  (:require [clojure.string :as str]
            [pi-matrix-relay.broker-client :as broker-client]
            [pi-matrix-relay.commands :as commands]
            [pi-matrix-relay.config :as config]
            [pi-matrix-relay.markdown :as markdown]
            [pi-matrix-relay.setup :as setup]))

(defn greeting [name]
  (str "Hello, " name ", from ClojureScript!"))

(defonce client-instance-id (str (random-uuid)))

(def default-deps
  {:health! broker-client/health!
   :register-client! broker-client/register-client!
   :update-subscriptions! broker-client/update-subscriptions!
   :get-room-delivery-mode! broker-client/get-room-delivery-mode!
   :set-room-delivery-mode! broker-client/set-room-delivery-mode!
   :get-room-prompt-mode! broker-client/get-room-prompt-mode!
   :set-room-prompt-mode! broker-client/set-room-prompt-mode!
   :get-room-tool-message-settings! broker-client/get-room-tool-message-settings!
   :set-room-tool-message-settings! broker-client/set-room-tool-message-settings!
   :heartbeat! broker-client/heartbeat!
   :unregister-client! broker-client/unregister-client!
   :acquire-slot! broker-client/acquire-slot!
   :release-slot! broker-client/release-slot!
   :list-slots! broker-client/list-slots!
   :list-rooms! broker-client/list-rooms!
   :open-event-stream! broker-client/open-event-stream!
   :resolve-room! broker-client/resolve-room!
   :send-message! broker-client/send-message!
   :set-typing! broker-client/set-typing!
   :send-reaction! broker-client/send-reaction!
   :set-interval! js/setInterval
   :clear-interval! js/clearInterval
   :set-timeout! js/setTimeout
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

(defn- dim-status-text
  [^js ctx status]
  (if (some? status)
    (try
      (if-let [theme (some-> ctx .-ui (aget "theme"))]
        (if-let [fg (aget theme "fg")]
          (fg "dim" status)
          status)
        status)
      (catch js/Error _
        status))
    js/undefined))

(defn- set-status!
  [^js ctx status]
  (when-let [ui (.-ui ctx)]
    (when-let [set-status (.-setStatus ui)]
      (set-status "pi-matrix-relay" (dim-status-text ctx status)))))

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
       (keep :room/id)
       distinct
       vec))

(defn- relay-subscriptions
  [project-config relay-state]
  (vec (distinct (cond-> (project-room-ids project-config)
                   (:room-id relay-state) (conj (:room-id relay-state))))))
(defn- effective-project-id
  [cwd project-config]
  (or (get-in project-config [:project :project/id])
      (config/project-id cwd)))

(defn- project-summary
  [cwd project-config]
  (let [project-id (effective-project-id cwd project-config)]
    {:project/root cwd
     :project/id project-id
     :project/display-name (or (get-in project-config [:project :project/display-name]) project-id)}))

(defn- now-iso []
  (.toISOString (js/Date.)))

(defn- error-summary
  [err]
  (cond-> {:message (or (.-message err) (str err))}
    (.-data err) (assoc :data (js->clj (.-data err) :keywordize-keys true))
    (.-code err) (assoc :code (.-code err))
    (.-stack err) (assoc :stack (.-stack err))))

(defn- record-diagnostic!
  ([diagnostics* type]
   (record-diagnostic! diagnostics* type {}))
  ([diagnostics* type details]
   (when diagnostics*
     (let [details (cond
                     (map? details) details
                     (nil? details) {}
                     :else {:message (str details)})
           event (merge {:at (now-iso)
                         :type (name type)}
                        details)]
       (swap! diagnostics* update :events
              (fnil (fn [events]
                      (vec (take-last 50 (conj (vec events) event))))
                    []))))
   nil))

(defn- pad2
  [n]
  (if (< n 10)
    (str "0" n)
    (str n)))

(defn- format-local-minute
  [timestamp]
  (if-not timestamp
    "unknown"
    (let [date (js/Date. timestamp)]
      (if (js/isNaN (.getTime date))
        "unknown"
        (str (.getFullYear date) "-"
             (pad2 (inc (.getMonth date))) "-"
             (pad2 (.getDate date)) " "
             (pad2 (.getHours date)) ":"
             (pad2 (.getMinutes date)))))))

(defn- home-path
  []
  (let [env (some-> js/globalThis
                    (aget "process")
                    (aget "env"))]
    (or (some-> env (aget "HOME"))
        (some-> env (aget "USERPROFILE")))))

(defn- shorten-home-path
  [path]
  (let [path (str path)
        home (home-path)]
    (cond
      (str/blank? path) "unknown"
      (and (seq home) (= path home)) "~"
      (and (seq home) (str/starts-with? path (str home "/")))
      (str "~/" (subs path (inc (count home))))

      :else path)))

(defn- session-status
  [relay-state]
  (or (:session/status relay-state)
      (cond
        (:session/ended-at relay-state) "ended"
        (:session/started-at relay-state) "started"
        :else nil)))

(defn- session-timestamp
  [relay-state status]
  (case status
    "ended" (or (:session/ended-at relay-state) (:session/started-at relay-state))
    "started" (:session/started-at relay-state)
    nil))

(defn- session-path
  [relay-state]
  (or (:session/path relay-state)
      (get-in relay-state [:project :project/root])))

(defn- session-summary-parts
  [relay-state]
  (when-let [status (session-status relay-state)]
    {:status status
     :time (format-local-minute (session-timestamp relay-state status))
     :path (shorten-home-path (session-path relay-state))}))

(defn- session-summary-text
  [relay-state]
  (if-let [{:keys [status time path]} (session-summary-parts relay-state)]
    (str status " " time " " path)
    "not recorded"))

(defn- status-text
  [project-config slot]
  (let [room-aliases (->> (room-bindings project-config)
                          (map :alias)
                          (remove str/blank?)
                          distinct
                          vec)
        base (str "[m]: slot " (or (:slot slot) "?"))]
    (if (seq room-aliases)
      (str base "; rooms: " (str/join ", " room-aliases))
      base)))

(defn- binding-for-room-id
  [project-config room-id]
  (some (fn [binding]
          (when (= room-id (:room/id binding))
            binding))
        (room-bindings project-config)))

(defn- slot-binding-for-room-id
  [relay-state room-id]
  (when (and (:room-id relay-state)
             (= room-id (:room-id relay-state)))
    {:alias (:room-name relay-state)
     :room/name (:room-name relay-state)
     :room/id (:room-id relay-state)
     :room/class "slot"
     :mode "all"
     :auto-reply? true
     :slot (:slot relay-state)}))

(defn- cached-room-delivery-mode
  [relay-state room-id]
  (when-let [delivery-modes* (:room-delivery-modes* relay-state)]
    (get @delivery-modes* room-id)))

(defn- effective-room-delivery-mode
  [relay-state room-id]
  (or (cached-room-delivery-mode relay-state room-id)
      {:delivery-mode config/default-delivery-mode
       :source "system-default"}))

(defn- cached-room-prompt-mode
  [relay-state room-id]
  (when-let [room-prompt-modes* (:room-prompt-modes* relay-state)]
    (get @room-prompt-modes* room-id)))

(defn- configured-room-prompt-mode
  [binding]
  (or (config/normalize-prompt-mode (:mode binding))
      config/default-room-prompt-mode))

(defn- effective-room-prompt-mode
  [relay-state binding room-id]
  (or (cached-room-prompt-mode relay-state room-id)
      {:mode (if (= "slot" (:room/class binding))
               "all"
               (configured-room-prompt-mode binding))
       :source (cond
                 (= "slot" (:room/class binding)) "slot-default"
                 (:mode binding) "project-config"
                 :else "system-default")}))

(defn- cached-room-tool-message-settings
  [relay-state room-id]
  (when-let [room-tool-message-settings* (:room-tool-message-settings* relay-state)]
    (get @room-tool-message-settings* room-id)))

(defn- default-tool-message-settings
  []
  {:enabled? config/default-tool-messages-enabled?
   :batch-ms config/default-tool-message-batch-ms
   :source "system-default"})

(defn- normalize-room-tool-message-settings
  [settings]
  (let [enabled? (cond
                   (contains? settings :enabled?) (:enabled? settings)
                   (contains? settings :room/tool-messages-enabled?) (:room/tool-messages-enabled? settings)
                   (contains? settings :tool-messages-enabled?) (:tool-messages-enabled? settings)
                   :else nil)
        batch-ms (cond
                   (contains? settings :batch-ms) (:batch-ms settings)
                   (contains? settings :room/tool-message-batch-ms) (:room/tool-message-batch-ms settings)
                   (contains? settings :tool-message-batch-ms) (:tool-message-batch-ms settings)
                   :else nil)]
    (cond-> {}
      (some? enabled?)
      (assoc :enabled? (boolean enabled?))

      (some? batch-ms)
      (assoc :batch-ms batch-ms)

      (:source settings)
      (assoc :source (:source settings)))))

(defn- effective-room-tool-message-settings
  [relay-state room-id]
  (merge (default-tool-message-settings)
         (cached-room-tool-message-settings relay-state room-id)))

(defn- cache-room-tool-message-settings!
  [relay-state room-id settings]
  (when-let [room-tool-message-settings* (:room-tool-message-settings* relay-state)]
    (let [normalized (merge (default-tool-message-settings)
                            (normalize-room-tool-message-settings settings)
                            {:source "broker"})]
      (swap! room-tool-message-settings* assoc room-id normalized)
      normalized)))

(defn- format-duration-ms
  [ms]
  (let [ms (or ms 0)]
    (cond
      (zero? (mod ms 1000)) (str (/ ms 1000) "s")
      :else (str ms "ms"))))

(defn- tool-message-settings-summary
  [relay-state room-id]
  (let [{:keys [enabled? batch-ms]} (effective-room-tool-message-settings relay-state room-id)]
    (str (if enabled? "on" "off") ", batch " (format-duration-ms batch-ms))))

(defn- binding-for-relay-room
  [relay-state room-id]
  (when-let [binding (or (slot-binding-for-room-id relay-state room-id)
                         (binding-for-room-id (:project-config relay-state) room-id))]
    (assoc binding :mode (:mode (effective-room-prompt-mode relay-state binding room-id)))))

(defn- authorized-sender?
  [{:keys [project-config global-operators]} sender]
  (let [global-operators (set global-operators)
        project-users (set (:allowed-users project-config))]
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

(def remote-command-prefixes ["/" "!"])

(defn- remote-command-text?
  [text]
  (let [trimmed (str/trim (str text))]
    (boolean (some #(str/starts-with? trimmed %) remote-command-prefixes))))

(defn- message-allowed-by-mode?
  [binding text bot-user-id]
  (case (or (:mode binding) config/default-room-prompt-mode)
    "all" true
    "mentions" (mentions-bot? text bot-user-id)
    "commands-only" (remote-command-text? text)
    false))

(defn- reaction-allowed-by-mode?
  [binding]
  (= "all" (or (:mode binding) config/default-room-prompt-mode)))

(defn- remote-command-body
  [trimmed]
  (cond
    (str/starts-with? trimmed "//") (subs trimmed 2)
    (str/starts-with? trimmed "/") (subs trimmed 1)
    (str/starts-with? trimmed "!") (subs trimmed 1)
    :else nil))

(defn- parse-remote-command
  [text]
  (let [trimmed (str/trim (str text))
        command-text (remote-command-body trimmed)]
    (when (seq command-text)
      (let [[name args] (str/split command-text #"\s+" 2)
            name (some-> name str/lower-case)
            args (str/trim (or args ""))]
        (when (seq name)
          {:name name
           :args args})))))

(defn- room-label
  ([binding]
   (room-label binding nil))
  ([binding room]
   (or (:alias binding)
       (:room/name binding)
       (:room/canonical-alias binding)
       (:room/name room)
       (:room/canonical-alias room)
       (:room/id binding)
       (:room/id room))))

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
  [project-config binding event]
  (let [label (room-label binding event)
        sender (or (:event/sender event) "unknown sender")
        timestamp (short-time (:event/timestamp event))
        text (or (:event/text event) "")
        include-room-id? (ambiguous-room-label? project-config binding event)
        metadata (metadata-lines [(str "eventId: " (:event/id event))]
                                 include-room-id?
                                 (:room/id event))]
    (str "Matrix " label " from " sender " at " timestamp "\n"
         text "\n\n"
         "Matrix metadata:\n"
         (str/join "\n" (cond-> metadata
                          (:event/reply-to-id event)
                          (conj (str "replyToEventId: " (:event/reply-to-id event))))))))

(defn- matrix-reaction-prompt
  [project-config binding event]
  (let [label (room-label binding event)
        sender (or (:event/sender event) "unknown sender")
        timestamp (short-time (:event/timestamp event))
        include-room-id? (ambiguous-room-label? project-config binding event)
        metadata (metadata-lines [(str "eventId: " (:event/id event))
                                  (str "reactsToEventId: " (:event/reacts-to-id event))]
                                 include-room-id?
                                 (:room/id event))]
    (str "Matrix reaction in " label " from " sender " at " timestamp "\n"
         "reacted " (:reaction/key event) " to event " (:event/reacts-to-id event) "\n\n"
         "Matrix metadata:\n"
         (str/join "\n" metadata))))

(defn- idle?
  [^js ctx]
  (if-let [is-idle (.-isIdle ctx)]
    (boolean (is-idle))
    true))

(defn- deliver-user-message!
  [^js pi ^js ctx relay-state room-id prompt]
  (if (idle? ctx)
    (do
      (.sendUserMessage pi prompt)
      true)
    (case (:delivery-mode (effective-room-delivery-mode relay-state room-id))
      "steer" (do
                (.sendUserMessage pi prompt #js {:deliverAs "steer"})
                true)
      "reject" (do
                 (notify! ctx "Matrix message ignored because the agent is busy." "warning")
                 false)
      (do
        (.sendUserMessage pi prompt #js {:deliverAs "followUp"})
        true))))

(defn- deliver-command-message!
  [^js pi prompt delivery-mode]
  (.sendUserMessage pi prompt #js {:deliverAs (if (= "steer" delivery-mode)
                                                "steer"
                                                "followUp")})
  true)

(defn- record-pending-auto-reply!
  [relay-state binding room-id event-id]
  (when (and (:auto-reply? binding) (:pending-auto-replies* relay-state))
    (swap! (:pending-auto-replies* relay-state) conj {:room-id room-id
                                                      :event-id event-id})))

(defn- message-body
  [message]
  (if (map? message)
    (or (:body message) (:message message) "")
    message))

(defn- message-formatted-body
  [message]
  (when (map? message)
    (:formatted-body message)))

(defn- send-room-ack!
  [{:keys [send-message! diagnostics*]} relay-state room-id event-id message]
  (when send-message!
    (-> (promise (send-message! room-id
                                (message-body message)
                                (cond-> {}
                                  (:client-id relay-state)
                                  (assoc :client/id (:client-id relay-state))

                                  event-id
                                  (assoc :reply-to/event-id event-id)

                                  (message-formatted-body message)
                                  (assoc :formatted-body (message-formatted-body message)))))
        (.catch (fn [err]
                  (record-diagnostic! diagnostics* :remote-command-ack-error (error-summary err))
                  nil)))))

(defn- debug-mode-enabled?
  [relay-state]
  (let [debug (get-in relay-state [:project-config :debug])]
    (cond
      (= false debug) false
      (map? debug) (and (not= false (:enabled debug))
                        (not= false (:modelContext debug))
                        (not= false (:model-context debug)))
      :else true)))

(defn- extension-error-notice
  [{:keys [source command room-id event-id sender]} err]
  (let [message (or (.-message err) (str err))]
    (str/join "\n"
              (cond-> ["pi-matrix-relay extension error"
                       "A relay extension error was recorded. Use matrix_relay_diagnostics for details."
                       (str "source: " (or source "extension"))]
                command (conj (str "command: /" command))
                room-id (conj (str "roomId: " room-id))
                event-id (conj (str "eventId: " event-id))
                sender (conj (str "sender: " sender))
                true (conj (str "error: " message))))))

(defn- inject-extension-error-notice!
  [{:keys [diagnostics*]} ^js pi relay-state details err]
  (when (debug-mode-enabled? relay-state)
    (try
      (when-let [send-user-message (.-sendUserMessage pi)]
        (send-user-message (extension-error-notice details err)
                           #js {:deliverAs "followUp"}))
      (catch js/Error inject-err
        (record-diagnostic! diagnostics* :debug-context-error (error-summary inject-err))))))

(defn- event-stream-close-error
  [state]
  (js/Error. (or (get-in state [:error/last :message])
                 (str "Event stream closed: " (:stream/close-reason state)))))

(defn- js->clj-safe
  [value]
  (try
    (cond
      (nil? value) nil
      (or (map? value) (sequential? value)) value
      :else (js->clj value :keywordize-keys true))
    (catch js/Error _
      nil)))

(defn- safe-invoke0
  [f]
  (try
    (when f
      (f))
    (catch js/Error _
      nil)))

(defn- safe-method0
  [obj method-name]
  (try
    (when-let [f (and obj (aget obj method-name))]
      (.call f obj))
    (catch js/Error _
      nil)))

(defn- format-count
  [n]
  (cond
    (nil? n) "?"
    (< n 1000) (str n)
    :else (str (.toFixed (/ n 1000) 1) "k")))

(defn- format-currency
  [n]
  (if (number? n)
    (str "$" (.toFixed n 3))
    "$0.000"))

(defn- html-escape
  [value]
  (str/escape (str value)
              {\& "&amp;"
               \< "&lt;"
               \> "&gt;"
               \" "&quot;"
               \' "&#39;"}))

(defn- session-summary-html
  [relay-state]
  (if-let [{:keys [status time path]} (session-summary-parts relay-state)]
    (str (html-escape status) " " (html-escape time) " <code>" (html-escape path) "</code>")
    "not recorded"))

(defn- ctx-model-value
  [^js ctx]
  (if-let [model (js->clj-safe (.-model ctx))]
    (str (:provider model) "/" (:id model))
    "none"))

(defn- ctx-context-value
  [^js ctx]
  (let [get-usage (.-getContextUsage ctx)
        usage (js->clj-safe (safe-invoke0 get-usage))
        model (js->clj-safe (.-model ctx))
        tokens (:tokens usage)
        context-window (or (:contextWindow usage) (:contextWindow model))
        percent (:percent usage)]
    (if (and tokens context-window (some? percent))
      (str tokens " tokens (" (js/Math.round percent) "%/" (.toFixed (/ context-window 1000) 0) "k)")
      "?")))

(defn- assistant-usage
  [entry]
  (let [entry (js->clj-safe entry)
        message (:message entry)]
    (when (and (= "message" (:type entry))
               (= "assistant" (:role message)))
      (:usage message))))

(defn- branch-usage
  [^js ctx]
  (when-let [session-manager (.-sessionManager ctx)]
    (when-let [branch (js->clj-safe (safe-method0 session-manager "getBranch"))]
      (reduce (fn [acc usage]
                (-> acc
                    (update :input + (or (:input usage) 0))
                    (update :output + (or (:output usage) 0))
                    (update :cost + (or (get-in usage [:cost :total]) 0))))
              {:input 0 :output 0 :cost 0}
              (keep assistant-usage branch)))))

(defn- ctx-usage-value
  [^js ctx]
  (let [{:keys [input output cost]} (or (branch-usage ctx) {:input 0 :output 0 :cost 0})]
    (str "↑" (format-count input) " ↓" (format-count output) " " (format-currency cost))))

(declare relay-progress-verbosity)
(declare tool-message-settings-summary)

(defn- remote-status-values
  [relay-state binding room-id ctx]
  (let [{:keys [delivery-mode source]} (effective-room-delivery-mode relay-state room-id)
        {room-prompt-mode :mode room-prompt-mode-source :source} (effective-room-prompt-mode relay-state binding room-id)]
    {:project (or (get-in relay-state [:project :project/id]) "unknown")
     :slot (or (:slot relay-state) "?")
     :slot-room (or (:room-name relay-state) "unknown")
     :room (or room-id "unknown")
     :session (session-summary-text relay-state)
     :session-html (session-summary-html relay-state)
     :room-prompt-mode room-prompt-mode
     :room-prompt-mode-source room-prompt-mode-source
     :delivery-mode delivery-mode
     :delivery-source source
     :tool-messages (tool-message-settings-summary relay-state room-id)
     :heartbeat (if (:heartbeat-id relay-state) "🟢" "🔴")
     :stream (if (:stream relay-state) "🟢" "🔴")
     :model (ctx-model-value ctx)
     :context (ctx-context-value ctx)
     :usage (ctx-usage-value ctx)}))

(defn- remote-status-body
  [{:keys [project slot slot-room room session room-prompt-mode room-prompt-mode-source delivery-mode delivery-source tool-messages heartbeat stream model context usage]}]
  (str "pi-matrix-relay status\n"
       "project: " project "\n"
       "slot: " slot " " slot-room "\n"
       "room: " room "\n"
       "session: " session "\n"
       "prompt mode: " room-prompt-mode " (" room-prompt-mode-source ")\n"
       "default delivery mode: " delivery-mode " (" delivery-source ")\n"
       "tool messages: " tool-messages "\n"
       "connection: heartbeat " heartbeat ", stream " stream "\n"
       "model: " model "\n"
       "context: " context "\n"
       "usage: " usage))

(defn- status-row
  [label value]
  (str "<tr><th>" (html-escape label) "</th><td>" value "</td></tr>"))

(defn- remote-status-html
  [{:keys [project slot slot-room room session-html room-prompt-mode room-prompt-mode-source delivery-mode delivery-source tool-messages heartbeat stream model context usage]}]
  (str "<h3>pi-matrix-relay status</h3>"
       "<table><tbody>"
       (status-row "Project" (str "<code>" (html-escape project) "</code>"))
       (status-row "Slot" (str "<code>" (html-escape slot) "</code> " (html-escape slot-room)))
       (status-row "Room" (str "<code>" (html-escape room) "</code>"))
       (status-row "Session" session-html)
       (status-row "Prompt mode" (str "<code>" (html-escape room-prompt-mode) "</code> <em>" (html-escape room-prompt-mode-source) "</em>"))
       (status-row "Default delivery mode" (str "<code>" (html-escape delivery-mode) "</code> <em>" (html-escape delivery-source) "</em>"))
       (status-row "Tool messages" (html-escape tool-messages))
       (status-row "Connection" (str "heartbeat " (html-escape heartbeat) ", stream " (html-escape stream)))
       (status-row "Model" (str "<code>" (html-escape model) "</code>"))
       (status-row "Context" (html-escape context))
       (status-row "Usage" (html-escape usage))
       "</tbody></table>"))

(defn- remote-status-message
  [relay-state binding room-id ctx]
  (let [values (remote-status-values relay-state binding room-id ctx)]
    {:body (remote-status-body values)
     :formatted-body (remote-status-html values)}))

(def typing-timeout-ms 30000)
(def typing-refresh-ms 20000)
(def tool-args-summary-limit 160)

(defn- relay-progress-verbosity
  [relay-state]
  (config/progress-verbosity (:project-config relay-state)))

(defn- progress-enabled?
  [relay-state]
  (not= "quiet" (relay-progress-verbosity relay-state)))

(defn- progress-verbose?
  [relay-state]
  (= "verbose" (relay-progress-verbosity relay-state)))

(defn- slot-room-id
  [relay-state]
  (:room-id relay-state))

(defn- truncate-text
  [text limit]
  (let [text (str text)]
    (if (> (count text) limit)
      (str (subs text 0 limit) "…")
      text)))

(defn- compact-pr-str
  [value]
  (try
    (-> (pr-str value)
        (str/replace #"\s+" " ")
        str/trim)
    (catch js/Error _
      "")))

(defn- tool-name-from-event
  [event]
  (or (:toolName event)
      (:tool-name event)
      "tool"))

(defn- tool-args-summary
  [event]
  (when-let [args (or (:args event) (:input event))]
    (let [summary (truncate-text (compact-pr-str args) tool-args-summary-limit)]
      (when-not (str/blank? summary)
        (str " " summary)))))

(defn- send-slot-progress!
  [{:keys [send-message! diagnostics*]} relay-state message]
  (if (and send-message!
           (progress-enabled? relay-state)
           (:client-id relay-state)
           (slot-room-id relay-state))
    (-> (promise (send-message! (slot-room-id relay-state)
                                message
                                {:client/id (:client-id relay-state)}))
        (.catch (fn [err]
                  (record-diagnostic! diagnostics* :progress/send-error (or (.-message err) (str err)))
                  nil)))
    (js/Promise.resolve nil)))

(defn- tool-message-batch-body
  [messages]
  (if (= 1 (count messages))
    (first messages)
    (str "🔧 Tools used:\n" (str/join "\n" (map #(str "- " %) messages)))))

(defn- clear-tool-message-batch!
  [relay-state room-id]
  (when-let [batches* (:tool-message-batches* relay-state)]
    (swap! batches* dissoc room-id)))

(defn- flush-tool-message-batch!
  [{:keys [send-message! diagnostics*]} relay-state room-id]
  (let [batches* (:tool-message-batches* relay-state)
        entry (when batches* (get @batches* room-id))
        messages (vec (:messages entry))]
    (when batches*
      (swap! batches* dissoc room-id))
    (if (and send-message!
             (seq messages)
             (:client-id relay-state)
             room-id)
      (-> (promise (send-message! room-id
                                  (tool-message-batch-body messages)
                                  {:client/id (:client-id relay-state)}))
          (.catch (fn [err]
                    (record-diagnostic! diagnostics* :tool-messages/send-error (or (.-message err) (str err)))
                    nil)))
      (js/Promise.resolve nil))))

(defn- queue-tool-message!
  [{:keys [set-timeout!] :as deps} relay-state room-id message]
  (let [{:keys [enabled? batch-ms]} (effective-room-tool-message-settings relay-state room-id)
        batches* (:tool-message-batches* relay-state)]
    (if (and enabled? batches* room-id)
      (let [needs-schedule? (atom false)]
        (swap! batches*
               (fn [batches]
                 (let [entry (get batches room-id)]
                   (if entry
                     (update-in batches [room-id :messages] conj message)
                     (do
                       (reset! needs-schedule? true)
                       (assoc batches room-id {:messages [message]}))))))
        (when @needs-schedule?
          (if set-timeout!
            (let [timeout-id (set-timeout! #(flush-tool-message-batch! deps relay-state room-id) batch-ms)]
              (swap! batches* update room-id assoc :timeout-id timeout-id))
            (flush-tool-message-batch! deps relay-state room-id)))
        (js/Promise.resolve nil))
      (js/Promise.resolve nil))))

(defn- set-slot-typing!
  [{:keys [set-typing! diagnostics*]} relay-state typing?]
  (if (and set-typing!
           (:client-id relay-state)
           (slot-room-id relay-state)
           (or typing? (progress-enabled? relay-state)))
    (-> (promise (set-typing! (slot-room-id relay-state)
                              typing?
                              {:client/id (:client-id relay-state)
                               :timeout/ms typing-timeout-ms}))
        (.catch (fn [err]
                  (record-diagnostic! diagnostics* :progress/typing-error (or (.-message err) (str err)))
                  nil)))
    (js/Promise.resolve nil)))

(defn- clear-slot-typing-interval!
  [{:keys [clear-interval!]} relay-state* relay-state]
  (when-let [interval-id (:typing-interval-id relay-state)]
    (when clear-interval!
      (clear-interval! interval-id))
    (when relay-state*
      (swap! relay-state* dissoc :typing-interval-id))))

(defn- start-slot-typing!
  [{:keys [set-interval!] :as deps} relay-state* relay-state]
  (if (progress-enabled? relay-state)
    (do
      (clear-slot-typing-interval! deps relay-state* relay-state)
      (let [start-promise (set-slot-typing! deps relay-state true)
            interval-id (when (and set-interval! relay-state*)
                          (set-interval!
                           (fn []
                             (when-let [state @relay-state*]
                               (when (progress-enabled? state)
                                 (set-slot-typing! deps state true))))
                           typing-refresh-ms))]
        (when interval-id
          (swap! relay-state* assoc :typing-interval-id interval-id))
        start-promise))
    (js/Promise.resolve nil)))

(defn- stop-slot-typing!
  [deps relay-state* relay-state]
  (clear-slot-typing-interval! deps relay-state* relay-state)
  (set-slot-typing! deps relay-state false))

(defn- handle-agent-start-progress!
  [deps relay-state* relay-state]
  (if (and relay-state (progress-enabled? relay-state))
    (js/Promise.all
     (clj->js [(start-slot-typing! deps relay-state* relay-state)
               (send-slot-progress! deps relay-state "Pi is working…")]))
    (js/Promise.resolve nil)))

(defn- handle-tool-start-progress!
  [deps relay-state event]
  (if relay-state
    (let [room-id (slot-room-id relay-state)
          tool-name (tool-name-from-event event)
          message (if (progress-verbose? relay-state)
                    (str "🔧 Tool started: " tool-name (or (tool-args-summary event) ""))
                    (str "🔧 " tool-name))]
      (queue-tool-message! deps relay-state room-id message))
    (js/Promise.resolve nil)))

(defn- handle-tool-end-progress!
  [deps relay-state event]
  (if (and relay-state (progress-verbose? relay-state))
    (let [room-id (slot-room-id relay-state)
          tool-name (tool-name-from-event event)
          error? (:isError event)
          icon (if error? "✗" "✓")
          status (if error? "failed" "finished")]
      (queue-tool-message! deps relay-state room-id (str icon " Tool " status ": " tool-name)))
    (js/Promise.resolve nil)))

(defn- cache-room-delivery-mode!
  [relay-state room-id delivery-mode]
  (when-let [delivery-modes* (:room-delivery-modes* relay-state)]
    (if (some? delivery-mode)
      (swap! delivery-modes* assoc room-id {:delivery-mode delivery-mode
                                            :source "broker"})
      (swap! delivery-modes* dissoc room-id))))

(defn- cache-room-prompt-mode!
  [relay-state room-id room-prompt-mode]
  (when-let [room-prompt-modes* (:room-prompt-modes* relay-state)]
    (if (some? room-prompt-mode)
      (swap! room-prompt-modes* assoc room-id {:mode room-prompt-mode
                                               :source "broker"})
      (swap! room-prompt-modes* dissoc room-id))))

(def remote-command-docs
  [{:command :help
    :names ["help"]
    :usage "help [command]"
    :summary "List commands or show command help."
    :details ["Without an argument, list Matrix relay commands."
              "With a command name, show detailed help for that command."]}
   {:command :status
    :names ["status"]
    :usage "status"
    :summary "Show relay status for this Pi session."
    :details ["Reports the project, slot room, prompt mode, delivery mode, tool message setting, heartbeat, stream, model, context, and usage."]}
   {:command :prompt
    :names ["prompt"]
    :usage "prompt <mode>"
    :summary "Set this room's prompt mode."
    :details ["Allowed modes: all, mentions, commands-only."
              "Use mentions to require addressing the bot before a message becomes a prompt."
              "Use commands-only to process remote commands while ignoring ordinary prompt messages."]}
   {:command :steer
    :names ["steer"]
    :usage "steer [message]"
    :summary "Steer a message into the current Pi turn, or set this room's default delivery mode to steer."
    :details ["With no message, set this room's default delivery mode to steer."
              "With a message, steer it into the current Pi turn."]}
   {:command :follow-up
    :names ["follow-up" "followup"]
    :usage "follow-up [message]"
    :summary "Queue a follow-up message, or set this room's default delivery mode to follow-up."
    :details ["With no message, set this room's default delivery mode to follow-up."
              "With a message, queue it as a follow-up after the current turn."]}
   {:command :reject
    :names ["reject"]
    :usage "reject"
    :summary "Set this room's default delivery mode to reject while Pi is busy."
    :details ["Reject ignores non-command Matrix messages while Pi is busy."]}
   {:command :tools
    :names ["tools"]
    :usage "tools <on|off|batch <duration>>"
    :summary "Configure this room's tool execution messages."
    :details ["Use tools on/off to enable or disable tool execution messages for this room."
              "Use tools batch <duration> to set the batching window, for example 30s, 60s, or 2m."
              "Current tool settings are shown in !status."]}
   {:command :abort
    :names ["abort"]
    :usage "abort"
    :summary "Abort the current Pi turn when supported by this context."
    :details ["Requests cancellation of the active Pi turn."]}
   {:command :compact
    :names ["compact"]
    :usage "compact [instructions]"
    :summary "Compact the current Pi conversation."
    :details ["Optionally include custom compaction instructions after the command."]}
   {:command :new
    :names ["new"]
    :usage "new"
    :summary "Start a new Pi session after the current turn is idle."
    :details ["Queues a new-session request and acknowledges when the new session starts."]}])

(def remote-command-doc-by-name
  (into {}
        (mapcat (fn [doc]
                  (map (fn [name] [name doc]) (:names doc)))
                remote-command-docs)))

(defn- normalize-command-target
  [target]
  (let [trimmed (str/lower-case (str/trim (str target)))]
    (or (remote-command-body trimmed)
        trimmed)))

(defn- remote-command-usage-line
  [usage]
  (str "!" usage))

(defn- formatted-message
  [body formatted-body]
  {:body body
   :formatted-body formatted-body})

(defn- remote-command-usage-html
  [usage]
  (str "<code>!" (html-escape usage) "</code>"))

(defn- remote-command-list-body
  []
  (str "Matrix relay commands\n"
       "Prefixes: / or !\n"
       (str/join "\n"
                 (map (fn [doc]
                        (str (remote-command-usage-line (:usage doc)) " — " (:summary doc)))
                      remote-command-docs))
       "\n\nUse !help <command> for details."))

(defn- remote-command-list-html
  []
  (str "<h3>Matrix relay commands</h3>"
       "<p><strong>Prefixes:</strong> <code>/</code> or <code>!</code></p>"
       "<table>"
       "<thead><tr><th>Command</th><th>Description</th></tr></thead>"
       "<tbody>"
       (str/join ""
                 (map (fn [doc]
                        (str "<tr><td>" (remote-command-usage-html (:usage doc)) "</td>"
                             "<td>" (html-escape (:summary doc)) "</td></tr>"))
                      remote-command-docs))
       "</tbody></table>"
       "<p>Use <code>!help &lt;command&gt;</code> for details.</p>"))

(defn- remote-command-list-message
  []
  (formatted-message (remote-command-list-body)
                     (remote-command-list-html)))

(defn- remote-command-detail-body
  [doc]
  (str "Matrix relay command: " (first (:names doc)) "\n"
       "Usage: " (remote-command-usage-line (:usage doc)) "\n"
       (when (< 1 (count (:names doc)))
         (str "Aliases: " (str/join ", " (rest (:names doc))) "\n"))
       (str/join "\n" (:details doc))))

(defn- remote-command-detail-html
  [doc]
  (str "<h3>Matrix relay command: <code>" (html-escape (first (:names doc))) "</code></h3>"
       "<p><strong>Usage:</strong> " (remote-command-usage-html (:usage doc)) "</p>"
       (when (< 1 (count (:names doc)))
         (str "<p><strong>Aliases:</strong> "
              (str/join ", " (map #(str "<code>" (html-escape %) "</code>") (rest (:names doc))))
              "</p>"))
       "<ul>"
       (str/join "" (map #(str "<li>" (html-escape %) "</li>") (:details doc)))
       "</ul>"))

(defn- remote-command-detail-message
  [doc]
  (formatted-message (remote-command-detail-body doc)
                     (remote-command-detail-html doc)))

(defn- unknown-command-message
  [target]
  (formatted-message (str "Unknown Matrix relay command: " target "\n\n"
                          (remote-command-list-body))
                     (str "<p>Unknown Matrix relay command: <code>" (html-escape target) "</code></p>"
                          (remote-command-list-html))))

(defn- remote-help-message
  [target]
  (let [target (normalize-command-target target)]
    (if (str/blank? target)
      (remote-command-list-message)
      (if-let [doc (get remote-command-doc-by-name target)]
        (remote-command-detail-message doc)
        (unknown-command-message target)))))

(defn- remote-command-name
  [name]
  (some-> name normalize-command-target remote-command-doc-by-name :command))

(defn- command-prompt-event
  [event text]
  (assoc event :event/text text))

(defn- message-entry-count
  [^js ctx]
  (if-let [session-manager (.-sessionManager ctx)]
    (count (filter #(= "message" (:type %))
                   (js->clj-safe (safe-method0 session-manager "getEntries"))))
    0))

(defn- handle-abort-command!
  [deps ctx relay-state room-id event-id]
  (if-let [abort (.-abort ctx)]
    (do
      (abort)
      (send-room-ack! deps relay-state room-id event-id "Abort requested.")
      true)
    (do
      (send-room-ack! deps relay-state room-id event-id "Abort is not available in this Pi context.")
      true)))

(defn- compact-complete-message
  [result]
  (let [result (js->clj-safe result)]
    (str "Compaction completed."
         (when-let [tokens-before (:tokensBefore result)]
           (str " Tokens before: " tokens-before ".")))))

(defn- handle-compact-command!
  [deps ctx relay-state room-id event-id args]
  (cond
    (< (message-entry-count ctx) 2)
    (do
      (send-room-ack! deps relay-state room-id event-id "Nothing to compact (no messages yet).")
      true)

    (not (.-compact ctx))
    (do
      (send-room-ack! deps relay-state room-id event-id "Compaction is not available in this Pi context.")
      true)

    :else
    (do
      ((.-compact ctx)
       #js {:customInstructions (when-not (str/blank? args) args)
            :onComplete (fn [result]
                          (send-room-ack! deps relay-state room-id event-id (compact-complete-message result)))
            :onError (fn [err]
                       (send-room-ack! deps relay-state room-id event-id
                                       (str "Compaction failed: " (or (.-message err) (str err)))))})
      (send-room-ack! deps relay-state room-id event-id "Compaction started.")
      true)))

(def internal-new-session-command "__new-session")

(defn- new-session-request-id
  []
  (str (random-uuid)))

(defn- queue-new-session-command!
  [deps ^js pi relay-state room-id event-id]
  (let [{:keys [pending-new-sessions*]} deps]
    (cond
      (nil? pending-new-sessions*)
      (do
        (send-room-ack! deps relay-state room-id event-id
                        "New session cannot be queued: command bridge is not initialized.")
        true)

      (not (.-sendUserMessage pi))
      (do
        (send-room-ack! deps relay-state room-id event-id
                        "New session cannot be queued: Pi message injection is not available.")
        true)

      :else
      (let [request-id (new-session-request-id)
            request {:room-id room-id
                     :event-id event-id
                     :requested-at (now-iso)}]
        (try
          (swap! pending-new-sessions* assoc request-id request)
          (.sendUserMessage pi
                            (str "/matrix-relay " internal-new-session-command " " request-id)
                            #js {:deliverAs "followUp"})
          (send-room-ack! deps relay-state room-id event-id
                          "New session requested. It will start after the current turn is idle.")
          true
          (catch js/Error err
            (swap! pending-new-sessions* dissoc request-id)
            (send-room-ack! deps relay-state room-id event-id
                            (str "New session could not be queued: " (or (.-message err) (str err))))
            true))))))

(defn- pop-pending-new-session!
  [pending-new-sessions* request-id]
  (let [request (get @pending-new-sessions* request-id)]
    (swap! pending-new-sessions* dissoc request-id)
    request))

(defn handle-internal-new-session-command!
  [{:keys [pending-new-sessions*] :as deps} request-id ^js ctx]
  (let [request (when pending-new-sessions*
                  (pop-pending-new-session! pending-new-sessions* request-id))]
    (cond
      (nil? request)
      (do
        (notify! ctx (str "No pending Matrix /new request for " request-id) "warning")
        (promise nil))

      (not (.-newSession ctx))
      (do
        (send-room-ack! deps nil (:room-id request) (:event-id request)
                        "New session is not available in this Pi command context.")
        (promise nil))

      :else
      (-> (promise (when-let [wait-for-idle (.-waitForIdle ctx)]
                     (wait-for-idle)))
          (.then (fn [_]
                   ((.-newSession ctx)
                    #js {:withSession (fn [_new-ctx]
                                        (send-room-ack! deps nil (:room-id request) (:event-id request)
                                                        "New session started."))})))
          (.then (fn [result]
                   (when (:cancelled (js->clj-safe result))
                     (send-room-ack! deps nil (:room-id request) (:event-id request)
                                     "New session cancelled."))
                   nil))
          (.catch (fn [err]
                    (record-diagnostic! (:diagnostics* deps) :new-session-error (error-summary err))
                    (send-room-ack! deps nil (:room-id request) (:event-id request)
                                    (str "New session failed: " (or (.-message err) (str err))))
                    nil))))))

(defn- handle-new-command!
  [deps pi relay-state room-id event-id]
  (queue-new-session-command! deps pi relay-state room-id event-id))

(defn- persist-room-delivery-mode-command!
  [{:keys [set-room-delivery-mode!] :as deps} relay-state room-id event-id sender delivery-mode]
  (if-not set-room-delivery-mode!
    (do
      (send-room-ack! deps relay-state room-id event-id
                      "Default delivery mode update failed: broker client is not available.")
      (promise true))
    (-> (promise (set-room-delivery-mode! (:client-id relay-state) room-id delivery-mode sender))
        (.then (fn [result]
                 (let [persisted-mode (or (:room/default-delivery-mode result) delivery-mode)]
                   (cache-room-delivery-mode! relay-state room-id persisted-mode)
                   (send-room-ack! deps relay-state room-id event-id
                                   (str "Default delivery mode for this room is now " persisted-mode "."))
                   true)))
        (.catch (fn [err]
                  (send-room-ack! deps relay-state room-id event-id
                                  (str "Default delivery mode update failed: " (or (.-message err) (str err))))
                  true)))))

(def prompt-mode-usage-message
  "Usage: !prompt <mode>\nAllowed modes: all, mentions, commands-only.")

(defn- persist-room-prompt-mode-command!
  [{:keys [set-room-prompt-mode!] :as deps} relay-state room-id event-id sender prompt-mode]
  (if-not set-room-prompt-mode!
    (do
      (send-room-ack! deps relay-state room-id event-id
                      "Prompt mode update failed: broker client is not available.")
      (promise true))
    (-> (promise (set-room-prompt-mode! (:client-id relay-state) room-id prompt-mode sender))
        (.then (fn [result]
                 (let [persisted-mode (or (:room/prompt-mode result) prompt-mode)]
                   (cache-room-prompt-mode! relay-state room-id persisted-mode)
                   (send-room-ack! deps relay-state room-id event-id
                                   (str "Prompt mode for this room is now " persisted-mode "."))
                   true)))
        (.catch (fn [err]
                  (send-room-ack! deps relay-state room-id event-id
                                  (str "Prompt mode update failed: " (or (.-message err) (str err))))
                  true)))))

(def min-tool-message-batch-ms 1000)
(def max-tool-message-batch-ms 3600000)

(def tools-usage-message
  "Usage: !tools <on|off|batch <duration>>\nExamples: !tools off, !tools on, !tools batch 30s")

(defn- parse-duration-ms
  [text]
  (let [text (str/lower-case (str/trim (str text)))]
    (when-let [[_ amount unit] (re-matches #"(\d+)(ms|s|m)?" text)]
      (let [n (js/parseInt amount 10)]
        (case (or unit "s")
          "ms" n
          "s" (* n 1000)
          "m" (* n 60000)
          nil)))))

(defn- valid-tool-message-batch-ms?
  [batch-ms]
  (and (int? batch-ms)
       (<= min-tool-message-batch-ms batch-ms max-tool-message-batch-ms)))

(defn- persist-room-tool-message-settings-command!
  [{:keys [set-room-tool-message-settings!] :as deps} relay-state room-id event-id sender settings ack-message]
  (if-not set-room-tool-message-settings!
    (do
      (send-room-ack! deps relay-state room-id event-id
                      "Tool message setting update failed: broker client is not available.")
      (promise true))
    (-> (promise (set-room-tool-message-settings! (:client-id relay-state) room-id settings sender))
        (.then (fn [result]
                 (let [cached-settings (cache-room-tool-message-settings! relay-state room-id result)]
                   (when (= false (:enabled? cached-settings))
                     (clear-tool-message-batch! relay-state room-id)))
                 (send-room-ack! deps relay-state room-id event-id ack-message)
                 true))
        (.catch (fn [err]
                  (send-room-ack! deps relay-state room-id event-id
                                  (str "Tool message setting update failed: " (or (.-message err) (str err))))
                  true)))))

(defn- handle-tools-command!
  [deps relay-state room-id event-id sender args]
  (let [[subcommand rest-args] (str/split (str/trim args) #"\s+" 2)
        subcommand (some-> subcommand str/lower-case)
        rest-args (str/trim (or rest-args ""))]
    (case subcommand
      "on"
      (persist-room-tool-message-settings-command! deps relay-state room-id event-id sender
                                                   {:enabled? true}
                                                   "Tool messages for this room are now on.")

      "off"
      (persist-room-tool-message-settings-command! deps relay-state room-id event-id sender
                                                   {:enabled? false}
                                                   "Tool messages for this room are now off.")

      "batch"
      (let [batch-ms (parse-duration-ms rest-args)]
        (if (valid-tool-message-batch-ms? batch-ms)
          (persist-room-tool-message-settings-command! deps relay-state room-id event-id sender
                                                       {:batch-ms batch-ms}
                                                       (str "Tool message batch window for this room is now " (format-duration-ms batch-ms) "."))
          (do
            (send-room-ack! deps relay-state room-id event-id tools-usage-message)
            true)))

      (do
        (send-room-ack! deps relay-state room-id event-id tools-usage-message)
        true))))
(defn- handle-remote-command!
  [deps pi ctx relay-state binding matrix-event]
  (let [room-id (:room/id matrix-event)
        event-id (:event/id matrix-event)
        {:keys [name args]} (parse-remote-command (:event/text matrix-event))
        command (remote-command-name name)]
    (if command
      (try
        (case command
          :help
          (do
            (send-room-ack! deps relay-state room-id event-id (remote-help-message args))
            true)

          :status
          (do
            (send-room-ack! deps relay-state room-id event-id (remote-status-message relay-state binding room-id ctx))
            true)

          :prompt
          (let [mode (config/normalize-prompt-mode args)]
            (if (config/valid-room-prompt-mode? mode)
              (persist-room-prompt-mode-command! deps relay-state room-id event-id (:event/sender matrix-event) mode)
              (do
                (send-room-ack! deps relay-state room-id event-id prompt-mode-usage-message)
                true)))

          :tools
          (handle-tools-command! deps relay-state room-id event-id (:event/sender matrix-event) args)

          :abort
          (handle-abort-command! deps ctx relay-state room-id event-id)

          :compact
          (handle-compact-command! deps ctx relay-state room-id event-id args)

          :new
          (handle-new-command! deps pi relay-state room-id event-id)

          :steer
          (if (str/blank? args)
            (persist-room-delivery-mode-command! deps relay-state room-id event-id (:event/sender matrix-event) "steer")
            (let [prompt-event (command-prompt-event matrix-event args)
                  prompt (matrix-message-prompt (:project-config relay-state) binding prompt-event)]
              (deliver-command-message! pi prompt "steer")
              (record-pending-auto-reply! relay-state binding room-id event-id)
              (send-room-ack! deps relay-state room-id event-id "Steering message sent.")
              true))

          :follow-up
          (if (str/blank? args)
            (persist-room-delivery-mode-command! deps relay-state room-id event-id (:event/sender matrix-event) "follow-up")
            (let [prompt-event (command-prompt-event matrix-event args)
                  prompt (matrix-message-prompt (:project-config relay-state) binding prompt-event)]
              (deliver-command-message! pi prompt "follow-up")
              (record-pending-auto-reply! relay-state binding room-id event-id)
              (send-room-ack! deps relay-state room-id event-id "Follow-up message queued.")
              true))

          :reject
          (if (str/blank? args)
            (persist-room-delivery-mode-command! deps relay-state room-id event-id (:event/sender matrix-event) "reject")
            (do
              (send-room-ack! deps relay-state room-id event-id
                              "Reject only changes the default delivery mode; omit message text.")
              true)))
        (catch js/Error err
          (record-diagnostic! (:diagnostics* deps)
                              :remote-command-error
                              (merge {:command name
                                      :room/id room-id
                                      :event/id event-id}
                                     (error-summary err)))
          (inject-extension-error-notice! deps pi relay-state {:source "remote-command"
                                                               :command name
                                                               :room-id room-id
                                                               :event-id event-id
                                                               :sender (:event/sender matrix-event)}
                                          err)
          (send-room-ack! deps relay-state room-id event-id
                          (str "Remote command /" name " failed: " (or (.-message err) (str err))))
          true))
      false)))

(defn- handle-broker-event-unsafe!
  [deps pi ctx relay-state event]
  (case (:type event)
    "matrix.message"
    (let [room-id (:room/id event)
          message event
          binding (binding-for-relay-room relay-state room-id)]
      (when (and binding
                 (not (:event/sender-is-bot? message))
                 (authorized-sender? relay-state (:event/sender message)))
        (let [command-result (handle-remote-command! deps pi ctx relay-state binding event)]
          (if command-result
            command-result
            (when (message-allowed-by-mode? binding (:event/text message) (:bot-user-id relay-state))
              (let [delivered? (deliver-user-message! pi ctx relay-state room-id (matrix-message-prompt (:project-config relay-state) binding event))]
                (when delivered?
                  (record-pending-auto-reply! relay-state binding room-id (:event/id message)))))))))

    "matrix.reaction"
    (let [room-id (:room/id event)
          reaction event
          binding (binding-for-relay-room relay-state room-id)]
      (when (and binding
                 (not (:event/sender-is-bot? reaction))
                 (authorized-sender? relay-state (:event/sender reaction))
                 (reaction-allowed-by-mode? binding))
        (deliver-user-message! pi ctx relay-state room-id (matrix-reaction-prompt (:project-config relay-state) binding event))))

    "broker.notice"
    (notify! ctx (:message event) (or (:level event) "info"))

    nil))

(defn handle-broker-event!
  [deps pi ctx relay-state event]
  (try
    (handle-broker-event-unsafe! deps pi ctx relay-state event)
    (catch js/Error err
      (let [matrix-event event
            room-id (:room/id event)]
        (record-diagnostic! (:diagnostics* deps)
                            :broker-event-error
                            (merge {:event/type (:type event)
                                    :room/id room-id
                                    :event/id (:event/id matrix-event)}
                                   (error-summary err)))
        (inject-extension-error-notice! deps pi relay-state {:source "broker-event"
                                                             :room-id room-id
                                                             :event-id (:event/id matrix-event)
                                                             :sender (:event/sender matrix-event)}
                                        err)
        nil))))

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

(defn- load-room-delivery-modes!
  [{:keys [get-room-delivery-mode! diagnostics*]} client-id room-ids]
  (let [room-ids (vec (distinct (remove str/blank? room-ids)))]
    (if-not get-room-delivery-mode!
      (promise (atom {}))
      (-> (js/Promise.all
           (clj->js
            (mapv (fn [room-id]
                    (-> (promise (get-room-delivery-mode! client-id room-id))
                        (.then (fn [result]
                                 {:room-id room-id
                                  :delivery-mode (:room/default-delivery-mode result)}))))
                  room-ids)))
          (.then (fn [results]
                   (let [cache (->> (js->clj results :keywordize-keys true)
                                    (keep (fn [{:keys [room-id delivery-mode]}]
                                            (when (some? delivery-mode)
                                              [room-id {:delivery-mode delivery-mode
                                                        :source "broker"}])))
                                    (into {}))]
                     (record-diagnostic! diagnostics* :room-delivery-modes-loaded {:rooms (keys cache)})
                     (atom cache))))))))

(defn- load-room-prompt-modes!
  [{:keys [get-room-prompt-mode! diagnostics*]} client-id room-ids]
  (let [room-ids (vec (distinct (remove str/blank? room-ids)))]
    (if-not get-room-prompt-mode!
      (promise (atom {}))
      (-> (js/Promise.all
           (clj->js
            (mapv (fn [room-id]
                    (-> (promise (get-room-prompt-mode! client-id room-id))
                        (.then (fn [result]
                                 {:room-id room-id
                                  :mode (:room/prompt-mode result)}))))
                  room-ids)))
          (.then (fn [results]
                   (let [cache (->> (js->clj results :keywordize-keys true)
                                    (keep (fn [{:keys [room-id mode]}]
                                            (when (some? mode)
                                              [room-id {:mode mode
                                                        :source "broker"}])))
                                    (into {}))]
                     (record-diagnostic! diagnostics* :room-prompt-modes-loaded {:rooms (keys cache)})
                     (atom cache))))))))

(defn- load-room-tool-message-settings!
  [{:keys [get-room-tool-message-settings! diagnostics*]} client-id room-ids]
  (let [room-ids (vec (distinct (remove str/blank? room-ids)))]
    (if-not get-room-tool-message-settings!
      (promise (atom {}))
      (-> (js/Promise.all
           (clj->js
            (mapv (fn [room-id]
                    (-> (promise (get-room-tool-message-settings! client-id room-id))
                        (.then (fn [result]
                                 (assoc (normalize-room-tool-message-settings result)
                                        :room-id room-id)))))
                  room-ids)))
          (.then (fn [results]
                   (let [cache (->> (js->clj results :keywordize-keys true)
                                    (keep (fn [{:keys [room-id] :as settings}]
                                            (let [normalized (normalize-room-tool-message-settings settings)]
                                              (when (or (contains? normalized :enabled?)
                                                        (contains? normalized :batch-ms))
                                                [room-id (merge (default-tool-message-settings)
                                                                normalized
                                                                {:source "broker"})]))))
                                    (into {}))]
                     (record-diagnostic! diagnostics* :room-tool-message-settings-loaded {:rooms (keys cache)})
                     (atom cache))))))))

(defn- reconnecting-stream-marker
  []
  #js {:diagnostics (fn []
                      (clj->js {:stream/reconnecting? true}))})

(defn- claim-empty-stream-slot!
  [relay-state* client-id marker]
  (loop []
    (let [relay-state @relay-state*]
      (cond
        (nil? relay-state)
        false

        (not= client-id (:client-id relay-state))
        false

        (:stream relay-state)
        false

        (compare-and-set! relay-state* relay-state (assoc relay-state :stream marker))
        true

        :else
        (recur)))))

(defn- replace-stream-marker!
  [relay-state* marker stream]
  (loop []
    (let [relay-state @relay-state*]
      (cond
        (not (identical? marker (:stream relay-state)))
        false

        (compare-and-set! relay-state*
                          relay-state
                          (if (some? stream)
                            (assoc relay-state :stream stream)
                            (dissoc relay-state :stream)))
        true

        :else
        (recur)))))

(defn- close-event-stream!
  [stream]
  (when stream
    (when-let [close (.-close stream)]
      (close))))

(defn- schedule-event-stream-reconnect!
  [{:keys [set-timeout! event-stream-reconnect-ms diagnostics*]} relay-state* client-id reopen!]
  (when (and relay-state*
             (= client-id (:client-id @relay-state*)))
    (let [delay-ms (or event-stream-reconnect-ms 1000)]
      (record-diagnostic! diagnostics* :event-stream-reconnect-scheduled {:client/id client-id
                                                                          :delay/ms delay-ms})
      (if set-timeout!
        (set-timeout! reopen! delay-ms)
        (reopen!)))))

(defn- open-relay-event-stream!
  ([deps pi ctx relay-state* client-id]
   (open-relay-event-stream! deps pi ctx relay-state* client-id false))
  ([{:keys [open-event-stream! diagnostics*] :as deps} pi ctx relay-state* client-id reconnect?]
   (when open-event-stream!
     (let [stream* (atom nil)
           reopen! (fn []
                     (let [marker (reconnecting-stream-marker)]
                       (when (claim-empty-stream-slot! relay-state* client-id marker)
                         (let [stream (open-relay-event-stream! deps pi ctx relay-state* client-id true)]
                           (if stream
                             (if (replace-stream-marker! relay-state* marker stream)
                               (record-diagnostic! diagnostics* :event-stream-reopened {:client/id client-id})
                               (close-event-stream! stream))
                             (replace-stream-marker! relay-state* marker nil))))))
           stream (open-event-stream!
                   {:on-error (fn [err]
                                (record-diagnostic! diagnostics* :event-stream-error (error-summary err)))
                    :on-open (fn [_state]
                               (when reconnect?
                                 (notify! ctx "Matrix relay event stream reconnected." "info")))
                    :on-close (fn [state]
                                (record-diagnostic! diagnostics* :event-stream-closed (select-keys state [:client/id
                                                                                                          :stream/connected?
                                                                                                          :stream/close-reason
                                                                                                          :close/requested?
                                                                                                          :error/last]))
                                (when-not (:close/requested? state)
                                  (when relay-state*
                                    (swap! relay-state*
                                           (fn [relay-state]
                                             (if (= (:stream relay-state) @stream*)
                                               (dissoc relay-state :stream)
                                               relay-state))))
                                  (when (:stream/connected? state)
                                    (inject-extension-error-notice! deps pi @relay-state* {:source "event-stream"} (event-stream-close-error state))
                                    (notify! ctx "Matrix relay event stream closed; reconnecting." "warning"))
                                  (schedule-event-stream-reconnect! deps relay-state* client-id reopen!)))}
                   client-id
                   (fn [event]
                     (when-let [relay-state (some-> relay-state* deref)]
                       (handle-broker-event! deps pi ctx relay-state event))))]
       (reset! stream* stream)
       stream))))

(defn start-relay!
  [{:keys [read-project-config! health! register-client! acquire-slot!
           update-subscriptions! diagnostics* relay-state*] :as deps}
   pi ctx]
  (let [cwd (ctx-cwd ctx)
        project-config (read-project-config! cwd)
        room-ids (project-room-ids project-config)
        project (project-summary cwd project-config)]
    (record-diagnostic! diagnostics* :start {:cwd cwd
                                             :project/id (:project/id project)})
    (-> (promise (health!))
        (.then
         (fn [health]
           (record-diagnostic! diagnostics* :health-ok {:matrix (select-keys health [:matrix/connected? :user/id])})
           (let [request {:client/instance-id client-instance-id
                          :protocol/version 1
                          :project (select-keys project [:project/root :project/id])
                          :subscriptions {:rooms room-ids}}]
             (-> (promise (register-client! request))
                 (.then
                  (fn [registration]
                    (let [client-id (:client/id registration)
                          global-operators (vec (:matrix/global-operators registration))]
                      (record-diagnostic! diagnostics* :client-registered {:client/id client-id})
                      (-> (promise (acquire-slot! client-id
                                                  (select-keys project [:project/id :project/display-name])
                                                  global-operators))
                          (.then
                           (fn [slot]
                             (record-diagnostic! diagnostics* :slot-acquired {:slot (:slot slot)
                                                                              :room/id (:room/id slot)
                                                                              :room/name (:room/name slot)})
                             (let [slot-room-id (:room/id slot)
                                   subscriptions (relay-subscriptions project-config {:room-id slot-room-id})
                                   started-at (now-iso)]
                               (-> (promise (update-subscriptions! client-id subscriptions))
                                   (.then
                                    (fn [_]
                                      (record-diagnostic! diagnostics* :subscriptions-updated {:client/id client-id
                                                                                               :rooms subscriptions})
                                      (js/Promise.all
                                       (clj->js [(load-room-delivery-modes! deps client-id subscriptions)
                                                 (load-room-prompt-modes! deps client-id subscriptions)
                                                 (load-room-tool-message-settings! deps client-id subscriptions)]))))
                                   (.then
                                    (fn [room-settings]
                                      (let [room-delivery-modes* (aget room-settings 0)
                                            room-prompt-modes* (aget room-settings 1)
                                            room-tool-message-settings* (aget room-settings 2)
                                            heartbeat-id (start-heartbeat! deps ctx client-id (:heartbeat/seconds registration))
                                            relay-state {:client-id client-id
                                                         :project-config project-config
                                                         :project project
                                                         :global-operators (set global-operators)
                                                         :bot-user-id (:user/id health)
                                                         :slot (:slot slot)
                                                         :room-id slot-room-id
                                                         :room-name (:room/name slot)
                                                         :room-delivery-modes* room-delivery-modes*
                                                         :room-prompt-modes* room-prompt-modes*
                                                         :room-tool-message-settings* room-tool-message-settings*
                                                         :tool-message-batches* (atom {})
                                                         :pending-auto-replies* (atom [])
                                                         :session/status "started"
                                                         :session/path cwd
                                                         :session/started-at started-at
                                                         :heartbeat-id heartbeat-id}
                                            relay-state* (or relay-state* (atom relay-state))
                                            _ (reset! relay-state* relay-state)
                                            stream (open-relay-event-stream! deps pi ctx relay-state* client-id)
                                            relay-state (assoc relay-state :stream stream)]
                                        (reset! relay-state* relay-state)
                                        (record-diagnostic! diagnostics* :event-stream-opened {:client/id client-id})
                                        (set-status! ctx (status-text project-config slot))
                                        relay-state))))))))))))))))))

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
  ([{:keys [clear-interval! release-slot! unregister-client!]} ctx relay-state]
   (when-let [stream (:stream relay-state)]
     (when-let [close (.-close stream)]
       (close)))
   (when-let [heartbeat-id (:heartbeat-id relay-state)]
     (when clear-interval!
       (clear-interval! heartbeat-id)))
   (-> (ignore-errors
        #(when (and release-slot! (:client-id relay-state) (:room-id relay-state) (:slot relay-state))
           (release-slot! (:client-id relay-state) (:room-id relay-state) (:slot relay-state))))
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
        text (assistant-final-text event)
        formatted-body (when text (markdown/markdown->matrix-html text))]
    (if (and send-message! target text)
      (-> (promise (send-message! (:room-id target)
                                  text
                                  {:client/id (:client-id relay-state)
                                   :reply-to/event-id (:event-id target)
                                   :formatted-body formatted-body}))
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

(defn- event-stream-diagnostic-flag
  [diagnostics names]
  (some #(get diagnostics %) names))

(defn- event-stream-active?
  [relay-state]
  (boolean
   (when (:stream relay-state)
     (let [diagnostics (stream-diagnostics relay-state)]
       (or (nil? diagnostics)
           (and (not (event-stream-diagnostic-flag diagnostics [:stream/closed? :closed?]))
                (not (event-stream-diagnostic-flag diagnostics [:close/requested? :requested?]))))))))

(defn- relay-snapshot
  [relay-state]
  (if relay-state
    (let [project-config (:project-config relay-state)
          pending* (:pending-auto-replies* relay-state)]
      (cond-> {:running true
               :client/id (:client-id relay-state)
               :project (:project relay-state)
               :project/rooms (project-room-ids project-config)
               :matrix/global-operators (vec (sort (:global-operators relay-state)))
               :user/id (:bot-user-id relay-state)
               :slot (:slot relay-state)
               :room/id (:room-id relay-state)
               :room/name (:room-name relay-state)
               :status/text (status-text project-config {:slot (:slot relay-state)
                                                         :room/name (:room-name relay-state)})
               :heartbeat/active? (boolean (:heartbeat-id relay-state))
               :stream/active? (event-stream-active? relay-state)
               :pending-auto-replies/count (if pending* (count @pending*) 0)}
        (:last-start-banner relay-state)
        (assoc :last-start-banner (:last-start-banner relay-state))
        (stream-diagnostics relay-state)
        (assoc :stream/diagnostics (stream-diagnostics relay-state))))
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

(defn- matrix-space-error-message
  [space]
  (or (get-in space [:error :message])
      "Matrix Space setup failed"))

(defn- matrix-space-error?
  [health]
  (= "error" (get-in health [:matrix/space :status])))

(defn- matrix-space-summary-line
  [health]
  (when-let [space (:matrix/space health)]
    (case (:status space)
      "error" (str "space: error " (matrix-space-error-message space))
      "ok" (str "space: " (or (:space/id space) "ok"))
      "disabled" "space: disabled"
      nil)))

(defn- broker-summary-line
  [broker]
  (let [health (:health broker)]
    (cond
      (:error health) (str "broker: error " (get-in health [:error :message]))
      (:matrix/connected? health) (str "broker: Matrix connected as " (:user/id health))
      (contains? health :matrix/connected?) "broker: Matrix not connected"
      :else "broker: not queried")))

(defn- slots-summary-line
  [broker]
  (if-let [slots (seq (get-in broker [:slots :slots]))]
    (str "slots: " (str/join ", " (map (fn [slot]
                                         (str (:slot slot) " " (:state slot) " " (:room/name slot)))
                                       slots)))
    "slots: none visible"))

(defn- diagnostic-error-event?
  [event]
  (str/ends-with? (str (:type event)) "-error"))

(defn- recent-error-events
  [events]
  (->> events
       (filter diagnostic-error-event?)
       (take-last 5)
       vec))

(defn- diagnostic-error-line
  [event]
  (let [label (or (:message event) (get-in event [:error :message]) "unknown error")]
    (str "- " (:type event) ": " label
         (when-let [command (:command event)]
           (str " command=/" command))
         (when-let [event-id (:event/id event)]
           (str " eventId=" event-id)))))

(defn- diagnostics-summary
  [{:keys [relay broker diagnostics]}]
  (str "pi-matrix-relay diagnostics\n"
       (if (:running relay)
         (str "extension: running slot " (:slot relay) " " (:room/name relay) "\n"
              "client: " (:client/id relay) "\n"
              "room: " (:room/id relay) "\n"
              "heartbeat: " (if (:heartbeat/active? relay) "active" "inactive") ", stream: "
              (if (:stream/active? relay) "active" "inactive") "\n")
         "extension: not running\n")
       (when-let [errors (seq (:recent-errors diagnostics))]
         (str "recent extension errors:\n"
              (str/join "\n" (map diagnostic-error-line errors))
              "\n"))
       (broker-summary-line broker) "\n"
       (when-let [space-line (matrix-space-summary-line (:health broker))]
         (str space-line "\n"))
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
                  (settled #(list-slots! (:project/id project)))
                  (promise {:ok true :value {:project/id (:project/id project) :slots []}}))
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
                       diagnostic-events (vec (:events (if diagnostics* @diagnostics* {})))
                       details {:cwd cwd
                                :project project
                                :project/config {:rooms (vec (room-bindings project-config))
                                                 :allowed-users (vec (:allowed-users project-config))}
                                :relay relay
                                :diagnostics {:events diagnostic-events
                                              :recent-errors (recent-error-events diagnostic-events)}
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
                     (record-diagnostic! diagnostics* :control-started {:client/id (:client-id relay-state)})
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
                     (record-diagnostic! diagnostics* :control-started {:client/id (:client-id relay-state)})
                     relay-state))
            (.then (fn [_]
                     (control-diagnostics deps ctx)))
            (.catch (fn [err]
                      (record-diagnostic! diagnostics* :control-restart-error (error-summary err))
                      (js/Promise.reject err)))))

      (js/Promise.reject (js/Error. (str "Unknown matrix relay control action: " action))))))

(def tui-help-text
  (str "Usage:\n"
       "  /mr [command]\n"
       "  /matrix-relay [command]\n\n"
       "Commands:\n"
       "  help                              Show this help.\n"
       "  setup                             Configure Matrix broker credentials.\n"
       "  status                            Show broker, slot, and listening-room status.\n"
       "  connect                           Connect this Pi extension instance to the broker.\n"
       "  disconnect                        Disconnect this Pi extension instance from the broker.\n"
       "  reconnect                         Reconnect this Pi extension instance to the broker.\n"
       "  room bind <room> [alias] [mode]    Bind a Matrix room alias/id to a local target.\n"
       "  room mode <target> <mode>          Set a bound prompt mode: all, mentions, commands-only.\n"
       "  progress <quiet|normal|verbose>  Configure slot-room typing/progress detail.\n"
       "  send <alias-or-room-id> <message>  Send a message to a bound room or raw room id.\n\n"
       "Examples:\n"
       "  /mr status\n"
       "  /mr room bind #ops:example.org ops mentions\n"
       "  /mr room mode ops commands-only\n"
       "  /mr progress normal\n"
       "  /mr send ops hello from Pi"))

(defn- handle-help!
  [ctx]
  (notify! ctx tui-help-text)
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

(defn- listening-room-label
  [binding]
  (let [labels (->> [(:alias binding)
                     (:room/name binding)
                     (:room/canonical-alias binding)]
                    (remove str/blank?)
                    distinct
                    vec)]
    (when (seq labels)
      (str " (" (str/join " / " labels) ")"))))

(defn- listening-room-lines
  [relay-state]
  (let [slot-room-id (:room-id relay-state)
        slot-line (when slot-room-id
                    (str "- " slot-room-id " (slot " (or (:room-name relay-state) (:slot relay-state) "?") ")"))
        project-lines (->> (room-bindings (:project-config relay-state))
                           (keep (fn [binding]
                                   (when-let [room-id (:room/id binding)]
                                     (when (not= room-id slot-room-id)
                                       (str "- " room-id (or (listening-room-label binding) "")))))))]
    (vec (cond-> []
           slot-line (conj slot-line)
           true (into project-lines)))))

(defn- tui-status-message
  [health relay-state]
  (let [broker-line (cond
                      (:matrix/connected? health)
                      (str "broker: Matrix connected as " (:user/id health))

                      (contains? health :matrix/connected?)
                      "broker: Matrix not connected"

                      :else
                      "broker: status unknown")
        space-line (matrix-space-summary-line health)]
    (str "Matrix relay status\n"
         broker-line "\n"
         (when space-line
           (str space-line "\n"))
         (if relay-state
           (str "extension: connected to broker\n"
                "project: " (or (get-in relay-state [:project :project/id]) "unknown") "\n"
                "slot: " (or (:slot relay-state) "?") " " (or (:room-name relay-state) "unknown") "\n"
                "slot room: " (or (:room-id relay-state) "unknown") "\n"
                "heartbeat: " (if (:heartbeat-id relay-state) "active" "inactive") "\n"
                "stream: " (if (event-stream-active? relay-state) "active" "inactive") "\n"
                "listening rooms:\n"
                (str/join "\n" (or (seq (listening-room-lines relay-state))
                                   ["- none"])))
           "extension: not connected to broker"))))

(defn- tui-status-level
  [health relay-state]
  (if (and (:matrix/connected? health)
           (not (matrix-space-error? health))
           relay-state
           (event-stream-active? relay-state))
    "info"
    "warning"))

(defn- handle-status!
  [{:keys [health! relay-state*]} ctx]
  (let [relay-state (some-> relay-state* deref)]
    (-> (promise (health!))
        (.then (fn [health]
                 (notify! ctx
                          (tui-status-message health relay-state)
                          (tui-status-level health relay-state)))))))

(defn- tui-control-action
  [action]
  (case action
    "connect" "start"
    "disconnect" "stop"
    "reconnect" "restart"
    action))

(defn- handle-control!
  [deps action ctx]
  (-> (execute-matrix-relay-control! deps
                                     {:action (tui-control-action action)}
                                     (or (:pi deps) #js {})
                                     ctx)
      (.then (fn [result]
               (notify! ctx (or (get-in result [:content 0 :text])
                                "Matrix relay control completed."))))))

(defn- hot-apply-project-config!
  [{:keys [relay-state* update-subscriptions! diagnostics*]} project-config]
  (if-let [relay-state (some-> relay-state* deref)]
    (let [subscriptions (relay-subscriptions project-config relay-state)]
      (-> (if (and update-subscriptions! (:client-id relay-state))
            (promise (update-subscriptions! (:client-id relay-state) subscriptions))
            (promise nil))
          (.then (fn [_]
                   (swap! relay-state* assoc :project-config project-config)
                   (record-diagnostic! diagnostics* :project-config-hot-applied
                                       {:client/id (:client-id relay-state)
                                        :rooms subscriptions})
                   nil))))
    (promise nil)))
(defn- handle-room-bind!
  [{:keys [resolve-room! read-project-config! write-project-config!] :as deps} {:keys [room alias mode]} ctx]
  (let [cwd (ctx-cwd ctx)]
    (if (and mode (not (config/valid-room-prompt-mode? mode)))
      (do
        (notify-error! ctx (str "Invalid prompt mode: " mode ". Allowed: all, mentions, commands-only."))
        (promise nil))
      (-> (promise (resolve-room! room))
          (.then (fn [room-result]
                   (let [old-config (read-project-config! cwd)
                         new-config (config/bind-room old-config room-result alias cwd mode)
                         binding (config/room-binding room-result alias cwd mode)]
                     (write-project-config! cwd new-config)
                     (-> (hot-apply-project-config! deps new-config)
                         (.then (fn [_]
                                  (notify! ctx (str "Bound " (:alias binding) " to " (:room/id binding) " with mode " (:mode binding)))))))))))))

(defn- update-room-binding-mode
  [project-config target mode]
  (update project-config :rooms
          (fn [rooms]
            (cond
              (map? rooms)
              (into {}
                    (map (fn [[k binding]]
                           [k (if (or (= target (str k))
                                      (= target (:alias binding))
                                      (= target (:room/id binding)))
                                (assoc binding :mode mode)
                                binding)]))
                    rooms)

              (sequential? rooms)
              (mapv (fn [binding]
                      (if (or (= target (:alias binding))
                              (= target (:room/id binding)))
                        (assoc binding :mode mode)
                        binding))
                    rooms)

              :else rooms))))

(defn- persist-broker-room-prompt-mode!
  [{:keys [set-room-prompt-mode! relay-state*]} room-id mode]
  (if-let [relay-state (some-> relay-state* deref)]
    (-> (promise (when set-room-prompt-mode!
                   (set-room-prompt-mode! (:client-id relay-state) room-id mode nil)))
        (.then (fn [result]
                 (cache-room-prompt-mode! relay-state room-id (or (:room/prompt-mode result) mode))
                 nil)))
    (promise nil)))

(defn- handle-room-prompt-mode!
  [{:keys [read-project-config! write-project-config!] :as deps} {:keys [target mode]} ctx]
  (let [cwd (ctx-cwd ctx)]
    (cond
      (not (config/valid-room-prompt-mode? mode))
      (do
        (notify-error! ctx (str "Invalid prompt mode: " mode ". Allowed: all, mentions, commands-only."))
        (promise nil))

      :else
      (let [project-config (read-project-config! cwd)]
        (if-let [binding (config/resolve-target project-config target)]
          (let [mode (config/normalize-prompt-mode mode)
                new-config (update-room-binding-mode project-config target mode)]
            (write-project-config! cwd new-config)
            (-> (persist-broker-room-prompt-mode! deps (:room/id binding) mode)
                (.then (fn [_]
                         (hot-apply-project-config! deps new-config)))
                (.then (fn [_]
                         (notify! ctx (str "Prompt mode for " target " is now " mode "."))))))
          (do
            (notify-error! ctx (str "No Matrix room binding for target " target))
            (promise nil)))))))

(defn- handle-progress-verbosity!
  [{:keys [read-project-config! write-project-config!] :as deps} {:keys [verbosity]} ctx]
  (let [cwd (ctx-cwd ctx)
        verbosity (config/normalize-progress-verbosity verbosity)]
    (if-not (config/valid-progress-verbosity? verbosity)
      (do
        (notify-error! ctx "Invalid progress verbosity. Allowed: quiet, normal, verbose.")
        (promise nil))
      (let [project-config (read-project-config! cwd)
            new-config (assoc-in project-config [:progress :verbosity] verbosity)]
        (write-project-config! cwd new-config)
        (-> (hot-apply-project-config! deps new-config)
            (.then (fn [_]
                     (notify! ctx (str "Matrix slot progress verbosity is now " verbosity ".")))))))))

(defn- handle-send!
  [{:keys [read-project-config! send-message!]} {:keys [target message]} ctx]
  (let [cwd (ctx-cwd ctx)
        project-config (read-project-config! cwd)]
    (if-let [binding (config/resolve-target project-config target)]
      (-> (promise (send-message! (:room/id binding) message))
          (.then (fn [result]
                   (notify! ctx (str "Sent Matrix message " (:event/id result))))))
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
       :control (handle-control! deps (:action command) ctx)
       :room-bind (handle-room-bind! deps command ctx)
       :room-prompt-mode (handle-room-prompt-mode! deps command ctx)
       :progress-verbosity (handle-progress-verbosity! deps command ctx)
       :send (handle-send! deps command ctx)
       :internal-new-session (handle-internal-new-session-command! deps (:request-id command) ctx)
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
         :room/id target})))

(def default-send-message-format "text/markdown")

(def ^:private supported-send-message-formats
  #{"text/markdown" "text/plain" "text/html"})

(defn- formatted-body-for-send-format
  [format message]
  (case format
    "text/markdown" (markdown/markdown->matrix-html message)
    "text/html" message
    "text/plain" nil))

(defn execute-send-matrix-message!
  [deps params ^js ctx]
  (let [{:keys [read-project-config! send-message! pi]} deps
        cwd (ctx-cwd ctx)
        target (:target params)
        message (:message params)
        format (or (:format params) default-send-message-format)
        reply-to-event-id (or (:replyToEventId params)
                              (:reply-to/event-id params))
        client-id (relay-client-id deps)]
    (cond
      (not (contains? supported-send-message-formats format))
      (js/Promise.reject (js/Error. (str "Unsupported Matrix message format " format "; use text/markdown, text/plain, or text/html.")))

      (contains? diagnostic-targets target)
      (execute-matrix-relay-diagnostics! deps {:includeBroker true
                                               :includeRooms true}
                                         ctx)

      (contains? control-targets target)
      (execute-matrix-relay-control! deps {:action (control-action message)} pi ctx)

      :else
      (let [project-config (read-project-config! cwd)
            formatted-body (formatted-body-for-send-format format message)]
        (if-let [binding (resolve-send-target project-config target)]
          (-> (promise (send-message! (:room/id binding)
                                      message
                                      (cond-> {}
                                        client-id (assoc :client/id client-id)
                                        reply-to-event-id (assoc :reply-to/event-id reply-to-event-id)
                                        formatted-body (assoc :formatted-body formatted-body))))
              (.then (fn [result]
                       {:content [{:type "text"
                                   :text (str "Sent Matrix message " (:event/id result)
                                              " to " (:room/id binding))}]
                        :details (cond-> {:room/id (:room/id binding)
                                          :event/id (:event/id result)
                                          :target target}
                                   reply-to-event-id (assoc :reply-to/event-id reply-to-event-id))})))
          (js/Promise.reject (js/Error. (str "No Matrix room binding for target " target))))))))

(defn execute-send-matrix-reaction!
  [deps params ^js ctx]
  (let [{:keys [read-project-config! send-reaction!]} deps
        cwd (ctx-cwd ctx)
        target (:target params)
        event-id (or (:eventId params)
                     (:event/id params))
        key (:key params)
        client-id (relay-client-id deps)
        project-config (read-project-config! cwd)]
    (if-let [binding (resolve-send-target project-config target)]
      (-> (promise (send-reaction! (:room/id binding)
                                   event-id
                                   key
                                   (cond-> {}
                                     client-id (assoc :client/id client-id))))
          (.then (fn [result]
                   {:content [{:type "text"
                               :text (str "Sent Matrix reaction " key " to " event-id
                                          " in " (:room/id binding))}]
                    :details {:room/id (:room/id binding)
                              :event/id (:event/id result)
                              :event/reacts-to-id event-id
                              :target target
                              :key key}})))
      (js/Promise.reject (js/Error. (str "No Matrix room binding for target " target))))))

(def send-message-format-description
  "text/markdown (default): render Markdown to Matrix HTML; text/plain: literal text; text/html: pre-rendered Matrix-safe HTML.")

(def send-matrix-message-parameters
  #js {:type "object"
       :additionalProperties false
       :required #js ["target" "message"]
       :properties #js {:target #js {:type "string"
                                     :description "Bound alias or Matrix room id"}
                        :message #js {:type "string"
                                      :description "Source content"}
                        :format #js {:type "string"
                                     :enum #js ["text/markdown" "text/plain" "text/html"]
                                     :default default-send-message-format
                                     :description send-message-format-description}
                        :replyToEventId #js {:type "string"
                                             :description "Matrix event id to reply to"}}})

(defn register-send-tool!
  [^js pi deps]
  (.registerTool pi
                 #js {:name "send_matrix_message"
                      :label "Send Matrix Message"
                      :description "Send a Matrix message. `message` content is interpreted by `format`; default is Markdown."
                      :promptSnippet "Send a Matrix message."
                      :promptGuidelines #js ["Use only when explicitly asked to message Matrix."
                                             "Omit format for Markdown; use text/plain for literal text, text/html for pre-rendered Matrix-safe HTML."
                                             "For Matrix replies, set replyToEventId."]
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
         pending-new-sessions* (atom {})
         deps (merge default-deps deps {:relay-state* relay-state*
                                        :diagnostics* diagnostics*
                                        :pending-new-sessions* pending-new-sessions*
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
       (on "agent_start"
           (fn [_event _ctx]
             (when-let [relay-state @relay-state*]
               (handle-agent-start-progress! deps relay-state* relay-state))))
       (on "tool_execution_start"
           (fn [event _ctx]
             (when-let [relay-state @relay-state*]
               (handle-tool-start-progress! deps relay-state (js->clj event :keywordize-keys true)))))
       (on "tool_execution_end"
           (fn [event _ctx]
             (when-let [relay-state @relay-state*]
               (handle-tool-end-progress! deps relay-state (js->clj event :keywordize-keys true)))))
       (on "agent_end"
           (fn [event _ctx]
             (when-let [relay-state @relay-state*]
               (js/Promise.all
                (clj->js [(stop-slot-typing! deps relay-state* relay-state)
                          (handle-agent-end! deps relay-state (js->clj event :keywordize-keys true))])))))
       (on "session_shutdown"
           (fn [_event ctx]
             (let [relay-state @relay-state*]
               (-> (when relay-state
                     (stop-slot-typing! deps relay-state* relay-state))
                   (promise)
                   (.then (fn [_]
                            (stop-relay! deps ctx relay-state)))
                   (.finally (fn []
                               (reset! relay-state* nil)))))))))))
