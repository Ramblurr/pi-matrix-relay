(ns pi-matrix-relay.setup
  (:require [clojure.string :as str]
            [pi-matrix-relay.broker-client :as broker-client]
            [pi-matrix-relay.config :as config]
            [pi-matrix-relay.systemd :as systemd]))

(def fs (js/require "fs"))

(defn parse-operators
  [text]
  (->> (str/split (or text "") #"[,\s]+")
       (map str/trim)
       (remove str/blank?)
       vec))

(defn config-from-fields
  [{:keys [homeserver-url user-id password operators encrypted?]}]
  {:matrix {:homeserver-url homeserver-url
            :user-id user-id
            :password password
            :operators (vec operators)
            :encrypted? (boolean encrypted?)
            :device-name "pi-matrix-relay-broker"}})

(defn write-global-config!
  [broker-config]
  (let [{:keys [config-path]} (config/global-paths)]
    (config/write-json-file! config-path broker-config)
    ;; Contains a password for the current Trixnity adapter, so keep it private.
    (.chmodSync fs config-path 384)
    broker-config))

(def default-deps
  {:write-global-config! write-global-config!
   :install-service! systemd/install-service!
   :health! broker-client/health!})

(defn- require-input
  [label value]
  (if (str/blank? (str value))
    (throw (js/Error. (str label " is required")))
    value))

(defn- status-text
  [health]
  (if-let [user-id (get-in health [:matrix :userId])]
    (str "matrix: " user-id)
    "matrix: disconnected"))

(defn run-setup!
  "Prompt for global broker config, optionally install the user service, and
  return broker health data. UI/system calls are injected for tests."
  [deps]
  (let [{:keys [input! editor! confirm! write-global-config! install-service! health! notify! set-status!]}
        (merge default-deps deps)]
    (-> (input! "Matrix homeserver URL" "https://matrix.example.org")
        (.then (fn [homeserver-url]
                 (-> (input! "Matrix bot user ID" "@pi:example.org")
                     (.then (fn [user-id]
                              [homeserver-url user-id])))))
        (.then (fn [[homeserver-url user-id]]
                 (-> (input! "Matrix bot password" "")
                     (.then (fn [password]
                              [homeserver-url user-id password])))))
        (.then (fn [[homeserver-url user-id password]]
                 (-> (editor! "Global operator MXIDs, one per line" "")
                     (.then (fn [operators-text]
                              [homeserver-url user-id password operators-text])))))
        (.then (fn [[homeserver-url user-id password operators-text]]
                 (-> (confirm! "Enable Matrix encryption?" "Encrypted rooms are the default for pi-matrix-relay.")
                     (.then (fn [encrypted?]
                              {:homeserver-url (require-input "Homeserver URL" homeserver-url)
                               :user-id (require-input "Matrix bot user ID" user-id)
                               :password (require-input "Matrix bot password" password)
                               :operators (parse-operators operators-text)
                               :encrypted? encrypted?})))))
        (.then (fn [fields]
                 (write-global-config! (config-from-fields fields))
                 (confirm! "Install and start broker service?" "Run systemctl --user enable --now pi-matrix-broker.service")))
        (.then (fn [install?]
                 (if install?
                   (install-service!)
                   (js/Promise.resolve nil))))
        (.then (fn [_]
                 (health!)))
        (.then (fn [health]
                 (let [status (status-text health)]
                   (when set-status!
                     (set-status! status))
                   (when notify!
                     (notify! (str "Matrix relay setup complete: " status) "info"))
                   health))))))
