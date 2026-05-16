(ns pi-matrix-relay.setup
  (:require [clojure.string :as str]
            [pi-matrix-relay.broker-client :as broker-client]
            [pi-matrix-relay.config :as config]
            [pi-matrix-relay.systemd :as systemd]))

(def fs (js/require "fs"))

(def mxid-pattern #"^@[^:\s]+:[^:\s]+$")

(defn parse-operators
  [text]
  (->> (str/split (or text "") #"[,\s]+")
       (map str/trim)
       (remove str/blank?)
       vec))

(defn mxid?
  [value]
  (boolean (and (string? value)
                (re-matches mxid-pattern value))))

(defn https-url?
  [value]
  (try
    (let [url (js/URL. value)]
      (and (= "https:" (.-protocol url))
           (not (str/blank? (.-hostname url)))))
    (catch js/Error _
      false)))

(defn validate-fields!
  [{:keys [homeserver-url user-id password operators] :as fields}]
  (cond
    (not (https-url? homeserver-url))
    (throw (js/Error. "Matrix homeserver URL must be an https:// URL."))

    (not (mxid? user-id))
    (throw (js/Error. "Matrix bot user ID must look like @user:server."))

    (str/blank? (str password))
    (throw (js/Error. "Matrix bot password is required."))

    :else
    (doseq [operator operators]
      (when-not (mxid? operator)
        (throw (js/Error. (str "Operator MXID must look like @user:server: " operator))))))
  fields)

(defn config-from-fields
  [{:keys [homeserver-url user-id password operators encrypted?]}]
  {:matrix {:homeserver-url homeserver-url
            :user-id user-id
            :password password
            :operators (vec operators)
            :encrypted? (boolean encrypted?)
            :device-name "pi-matrix-relay-broker"}})

(defn read-global-config!
  []
  (or (config/read-json-file! (:config-path (config/global-paths))) {}))

(defn write-global-config!
  [broker-config]
  (let [{:keys [config-path]} (config/global-paths)]
    (config/write-json-file! config-path broker-config)
    ;; Contains a password for the current Trixnity adapter, so keep it private.
    (.chmodSync fs config-path 384)
    broker-config))

(defn sleep!
  [ms]
  (js/Promise.
   (fn [resolve _reject]
     (js/setTimeout resolve ms))))

(def default-deps
  {:read-global-config! read-global-config!
   :write-global-config! write-global-config!
   :install-service! systemd/install-service!
   :health! broker-client/health!
   :sleep! sleep!
   :health-attempts 10
   :health-delay-ms 1000})

(defn- keep-blank
  [value fallback]
  (let [trimmed (str/trim (str value))]
    (if (str/blank? trimmed)
      fallback
      trimmed)))

(defn- password-placeholder
  [existing-password]
  (if (str/blank? (str existing-password))
    ""
    "leave blank to keep existing password"))

(defn- status-text
  [health]
  (if-let [user-id (get-in health [:matrix :userId])]
    (str "matrix: " user-id)
    "matrix: disconnected"))

(defn- connected?
  [health]
  (true? (get-in health [:matrix :connected])))

(defn poll-health!
  [{:keys [health! sleep! health-attempts health-delay-ms]}]
  (let [max-attempts (or health-attempts 10)
        delay-ms (or health-delay-ms 1000)]
    (letfn [(attempt [n]
              (-> (health!)
                  (.then (fn [health]
                           (if (connected? health)
                             health
                             (if (< n max-attempts)
                               (-> (sleep! delay-ms)
                                   (.then (fn [_] (attempt (inc n)))))
                               (throw (js/Error. "Matrix broker did not report connected after setup."))))))))]
      (attempt 1))))

(defn- collect-fields!
  [{:keys [input! editor! confirm!]} existing]
  (let [existing-matrix (:matrix existing)
        existing-homeserver (:homeserver-url existing-matrix)
        existing-user-id (:user-id existing-matrix)
        existing-password (:password existing-matrix)
        existing-operators (:operators existing-matrix)
        existing-encrypted? (not (false? (:encrypted? existing-matrix)))]
    (-> (input! "Matrix homeserver URL" (or existing-homeserver "https://matrix.example.org"))
        (.then (fn [homeserver-url]
                 (-> (input! "Matrix bot user ID" (or existing-user-id "@pi:example.org"))
                     (.then (fn [user-id]
                              [homeserver-url user-id])))))
        (.then (fn [[homeserver-url user-id]]
                 (-> (input! "Matrix bot password" (password-placeholder existing-password))
                     (.then (fn [password]
                              [homeserver-url user-id password])))))
        (.then (fn [[homeserver-url user-id password]]
                 (-> (editor! "Global operator MXIDs, one per line" (str/join "\n" existing-operators))
                     (.then (fn [operators-text]
                              [homeserver-url user-id password operators-text])))))
        (.then (fn [[homeserver-url user-id password operators-text]]
                 (-> (confirm! "Enable Matrix encryption?"
                              (str "Encrypted rooms are the default for pi-matrix-relay. Current: "
                                   (if existing-encrypted? "enabled" "disabled")))
                     (.then (fn [encrypted?]
                              (validate-fields!
                               {:homeserver-url (keep-blank homeserver-url existing-homeserver)
                                :user-id (keep-blank user-id existing-user-id)
                                :password (keep-blank password existing-password)
                                :operators (parse-operators operators-text)
                                :encrypted? encrypted?})))))))))

(defn run-setup!
  "Prompt for global broker config, optionally install the user service, and
  return broker health data. UI/system calls are injected for tests."
  [deps]
  (let [{:keys [read-global-config! write-global-config! install-service! notify! set-status!]
         :as deps}
        (merge default-deps deps)]
    (-> (js/Promise.resolve (read-global-config!))
        (.then (fn [existing]
                 (collect-fields! deps existing)))
        (.then (fn [fields]
                 (write-global-config! (config-from-fields fields))
                 ((:confirm! deps) "Install and start broker service?"
                  "Run systemctl --user enable --now pi-matrix-broker.service")))
        (.then (fn [install?]
                 (if install?
                   (install-service!)
                   (js/Promise.resolve nil))))
        (.then (fn [_]
                 (poll-health! deps)))
        (.then (fn [health]
                 (let [status (status-text health)]
                   (when set-status!
                     (set-status! status))
                   (when notify!
                     (notify! (str "Matrix relay setup complete: " status) "info"))
                   health)))
        (.catch (fn [err]
                  (when notify!
                    (notify! (.-message err) "error"))
                  (throw err))))))
