(ns pi-matrix-relay.systemd
  (:require [clojure.string :as str]))

(def fs (js/require "fs"))
(def path (js/require "path"))
(def child-process (js/require "child_process"))
(def os (js/require "os"))

(def service-name "pi-matrix-broker.service")

(defn- env-value
  [env k]
  (aget (or env #js {}) k))

(defn service-path
  ([]
   (service-path (.-env js/process)))
  ([env]
   (.join path
          (or (env-value env "XDG_CONFIG_HOME")
              (.join path (or (env-value env "HOME") (.homedir os)) ".config"))
          "systemd"
          "user"
          service-name)))

(defn render-service
  [{:keys [broker-dir]}]
  (str/join
   "\n"
   ["[Unit]"
    "Description=Pi Matrix Relay broker"
    "After=network-online.target"
    ""
    "[Service]"
    "Type=simple"
    (str "WorkingDirectory=" broker-dir)
    "ExecStart=/usr/bin/env bb broker"
    "Restart=on-failure"
    "RestartSec=3"
    ""
    "[Install]"
    "WantedBy=default.target"
    ""]))

(defn default-broker-dir
  []
  ;; Repository-local spike layout: extension/dist/pi-matrix-relay.js -> ../../broker.
  (.resolve path js/__dirname ".." ".." "broker"))

(defn write-service!
  ([]
   (write-service! {:broker-dir (default-broker-dir)}))
  ([opts]
   (let [file (service-path)
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
   (install-service! {:broker-dir (default-broker-dir)}))
  ([opts]
   (write-service! opts)
   (-> (systemctl-user! "daemon-reload")
       (.then (fn [_]
                (systemctl-user! "enable" "--now" service-name))))))
