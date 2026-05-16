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
   (doseq [command-name ["matrix-relay" "mr"]]
     (.registerCommand pi command-name
       #js {:description "Control the Pi Matrix relay"
            :handler (fn [args ctx]
                       (handle-command! deps args ctx))}))
   (register-send-tool! pi deps)))
