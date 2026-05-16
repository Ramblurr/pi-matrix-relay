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

(defn- nonblank-string
  [value]
  (let [trimmed (str/trim (str value))]
    (when-not (str/blank? trimmed)
      trimmed)))

(defn- has-url-scheme?
  [value]
  (boolean (re-find #"^[A-Za-z][A-Za-z0-9+.-]*://" value)))

(defn- homeserver-prefill
  [value]
  (when-let [trimmed (nonblank-string value)]
    (if (has-url-scheme? trimmed)
      trimmed
      (str "https://" trimmed))))

(defn- validate-homeserver!
  [homeserver-url]
  (when-not (https-url? homeserver-url)
    (throw (js/Error. "Matrix homeserver URL must be an https:// URL.")))
  homeserver-url)

(defn- validate-user-id!
  [user-id]
  (when-not (mxid? user-id)
    (throw (js/Error. "Matrix bot user ID must look like @user:server.")))
  user-id)

(defn- validate-password!
  [password]
  (when (str/blank? (str password))
    (throw (js/Error. "Matrix bot password is required.")))
  password)

(defn- validate-operators!
  [operators]
  (doseq [operator operators]
    (when-not (mxid? operator)
      (throw (js/Error. (str "Operator MXID must look like @user:server: " operator)))))
  operators)

(defn- validate-operator-text!
  [operator-text]
  (validate-operators! (parse-operators operator-text)))

(defn validate-fields!
  [{:keys [homeserver-url user-id password operators] :as fields}]
  (validate-homeserver! homeserver-url)
  (validate-user-id! user-id)
  (validate-password! password)
  (validate-operators! operators)
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

(defn- password-label
  [existing-password]
  (if (str/blank? (str existing-password))
    "Matrix bot password"
    "Matrix bot password (leave blank to keep existing password)"))

(defn- prompt-valid!
  [{:keys [notify!]} prompt! label initial validate!]
  (letfn [(ask [current-initial]
            (-> (prompt! label current-initial)
                (.then (fn [value]
                         (try
                           (validate! value)
                           (catch js/Error err
                             (when notify!
                               (notify! (.-message err) "error"))
                             (ask (str value))))))))]
    (ask initial)))

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
  [{:keys [input! editor! confirm! notify!]} existing]
  (let [existing-matrix (:matrix existing)
        existing-homeserver (homeserver-prefill (:homeserver-url existing-matrix))
        existing-user-id (:user-id existing-matrix)
        existing-password (:password existing-matrix)
        existing-operators (:operators existing-matrix)
        existing-encrypted? (not (false? (:encrypted? existing-matrix)))]
    (-> (prompt-valid! {:notify! notify!}
                       editor!
                       "Matrix homeserver URL"
                       (or existing-homeserver "https://matrix.example.org")
                       #(validate-homeserver! (keep-blank % existing-homeserver)))
        (.then (fn [homeserver-url]
                 (-> (prompt-valid! {:notify! notify!}
                                    editor!
                                    "Matrix bot user ID"
                                    (or existing-user-id "@pi:example.org")
                                    #(validate-user-id! (keep-blank % existing-user-id)))
                     (.then (fn [user-id]
                              [homeserver-url user-id])))))
        (.then (fn [[homeserver-url user-id]]
                 (-> (prompt-valid! {:notify! notify!}
                                    input!
                                    (password-label existing-password)
                                    ""
                                    #(validate-password! (keep-blank % existing-password)))
                     (.then (fn [password]
                              [homeserver-url user-id password])))))
        (.then (fn [[homeserver-url user-id password]]
                 (-> (prompt-valid! {:notify! notify!}
                                    editor!
                                    "Global operator MXIDs, one per line"
                                    (str/join "\n" existing-operators)
                                    validate-operator-text!)
                     (.then (fn [operators]
                              [homeserver-url user-id password operators])))))
        (.then (fn [[homeserver-url user-id password operators]]
                 (-> (confirm! "Enable Matrix encryption?"
                              (str "Encrypted rooms are the default for pi-matrix-relay. Current: "
                                   (if existing-encrypted? "enabled" "disabled")))
                     (.then (fn [encrypted?]
                              (validate-fields!
                               {:homeserver-url homeserver-url
                                :user-id user-id
                                :password password
                                :operators operators
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
