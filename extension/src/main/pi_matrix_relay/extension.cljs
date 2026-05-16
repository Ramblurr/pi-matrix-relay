(ns pi-matrix-relay.extension
  (:require [clojure.string :as str]
            [pi-matrix-relay.broker-client :as broker-client]
            [pi-matrix-relay.commands :as commands]
            [pi-matrix-relay.config :as config]))

(defn greeting [name]
  (str "Hello, " name ", from ClojureScript!"))

(def default-deps
  {:health! broker-client/health!
   :resolve-room! broker-client/resolve-room!
   :send-message! broker-client/send-message!
   :read-project-config! config/read-project-config!
   :write-project-config! config/write-project-config!})

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
  [ctx]
  (notify! ctx "Matrix relay setup is not wired yet; edit ~/.config/pi-matrix-relay/config.json for now." "warning")
  (promise nil))

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
       :setup (handle-setup! ctx)
       :status (handle-status! deps ctx)
       :room-bind (handle-room-bind! deps command ctx)
       :send (handle-send! deps command ctx)
       :error (do
                (notify-error! ctx message)
                (promise nil))))))

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
                       (handle-command! deps args ctx))}))))
