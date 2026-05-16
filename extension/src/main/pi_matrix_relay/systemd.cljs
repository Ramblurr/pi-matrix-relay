(ns pi-matrix-relay.systemd
  (:require [clojure.string :as str]
            [ol.dirs :as dirs]))

(def fs (js/require "fs"))
(def path (js/require "path"))
(def child-process (js/require "child_process"))
(def os (js/require "os"))

(def service-name "pi-matrix-broker.service")

(declare default-broker-dir)

(defn- env-value
  [env k]
  (aget (or env #js {}) k))

(defn service-path
  ([]
   (.join path (dirs/config-dir "systemd") "user" service-name))
  ([env]
   (.join path
          (or (env-value env "XDG_CONFIG_HOME")
              (.join path (or (env-value env "HOME") (.homedir os)) ".config"))
          "systemd"
          "user"
          service-name)))

(defn- command-path
  [env-path command]
  (some (fn [dir]
          (let [candidate (.join path dir command)]
            (when (.existsSync fs candidate)
              candidate)))
        (str/split (or env-path "") #":")))

(defn service-opts
  ([]
   (service-opts (.-env js/process)))
  ([env]
   (let [path-env (or (env-value env "PATH") "")]
     {:broker-dir (default-broker-dir)
      :bb-command (or (command-path path-env "bb") "/usr/bin/env bb")
      :path-env path-env})))

(defn render-service
  [{:keys [broker-dir bb-command path-env]
    :or {bb-command "/usr/bin/env bb"}}]
  (str/join
   "\n"
   (cond-> ["[Unit]"
            "Description=Pi Matrix Relay broker"
            "After=network-online.target"
            ""
            "[Service]"
            "Type=simple"]
     (seq path-env) (conj (str "Environment=PATH=" path-env))
     true (conj (str "WorkingDirectory=" broker-dir)
                (str "ExecStart=" bb-command " broker")
                "Restart=on-failure"
                "RestartSec=3"
                ""
                "[Install]"
                "WantedBy=default.target"
                ""))))

(defn default-broker-dir
  []
  ;; Repository-local spike layout: extension/dist/pi-matrix-relay.js -> ../../broker.
  (.resolve path js/__dirname ".." ".." "broker"))

(defn write-service!
  ([]
   (write-service! (service-opts)))
  ([opts]
   (let [opts (merge (service-opts) opts)
         file (service-path)
         service (render-service opts)]
     (.mkdirSync fs (.dirname path file) #js {:recursive true})
     (.writeFileSync fs file service "utf8")
     file)))

(defn exec-file!
  [cmd args]
  (js/Promise.
   (fn [resolve reject]
     (.execFile child-process cmd (clj->js args)
                (fn [^js err stdout stderr]
                  (if err
                    (do
                      (set! (.-stdout err) stdout)
                      (set! (.-stderr err) stderr)
                      (reject err))
                    (resolve {:stdout stdout
                              :stderr stderr})))))))

(defn systemctl-user!
  [& args]
  (exec-file! "systemctl" (into ["--user"] args)))

(defn install-service!
  ([]
   (install-service! (service-opts)))
  ([opts]
   (write-service! opts)
   (-> (systemctl-user! "daemon-reload")
       (.then (fn [_]
                (systemctl-user! "enable" "--now" service-name))))))
