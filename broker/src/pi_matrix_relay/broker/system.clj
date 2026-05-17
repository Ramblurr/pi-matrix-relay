(ns pi-matrix-relay.broker.system
  (:require [donut.system :as ds]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.config :as config]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.http :as http]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.paths :as paths]
            [pi-matrix-relay.broker.state :as state])
  (:import [java.io File RandomAccessFile]
           [java.nio.channels OverlappingFileLockException]
           [java.time Instant]))

(defn- close-quietly!
  [resource]
  (when resource
    (try
      (.close resource)
      (catch Throwable _
        nil))))

(defn- start-process-lock!
  [{:keys [paths]}]
  (let [lock-path (:lock-path paths)
        lock-file (File. lock-path)
        raf (RandomAccessFile. lock-file "rw")
        channel (.getChannel raf)]
    (try
      (if-let [lock (.tryLock channel)]
        (do
          (.setLength raf 0)
          (.writeBytes raf (str (.pid (java.lang.ProcessHandle/current)) "\n"))
          {:path lock-path
           :file lock-file
           :raf raf
           :channel channel
           :lock lock})
        (throw (ex-info "pi-matrix-relay broker is already running."
                        {:code :broker_already_running
                         :lock-path lock-path})))
      (catch OverlappingFileLockException _
        (close-quietly! channel)
        (close-quietly! raf)
        (throw (ex-info "pi-matrix-relay broker is already running."
                        {:code :broker_already_running
                         :lock-path lock-path})))
      (catch Throwable t
        (close-quietly! channel)
        (close-quietly! raf)
        (throw t)))))

(defn- stop-process-lock!
  [{:keys [lock channel raf file]}]
  (when lock
    (try
      (.release lock)
      (catch Throwable _
        nil)))
  (close-quietly! channel)
  (close-quietly! raf)
  (when file
    (try
      (.delete ^File file)
      (catch Throwable _
        nil))))

(defn stale-client-notice
  [lease]
  (str "pi-matrix-relay slot " (:slot lease)
       " client disconnected unexpectedly. Last heartbeat: "
       (str (Instant/ofEpochMilli (:last-heartbeat-at lease)))
       ". Slot lease released."))

(defn sweep-stale-leases!
  [{:keys [state* config matrix-gateway now]}]
  (let [heartbeat-seconds (get-in config [:leases :heartbeat-seconds] 30)
        stale-after-missed (get-in config [:leases :stale-after-missed] 3)
        stale (state/mark-stale-leases! state*
                                         (or now (System/currentTimeMillis))
                                         heartbeat-seconds
                                         stale-after-missed)]
    (doseq [lease stale]
      (try
        (matrix/send-message! matrix-gateway
                              {:target {:roomId (:room-id lease)}
                               :body (stale-client-notice lease)})
        (catch Throwable _
          nil)))
    stale))

(defn- start-sweeper!
  [{:keys [config] :as sweep-config}]
  (let [running? (atom true)
        heartbeat-seconds (get-in config [:leases :heartbeat-seconds] 30)
        thread (Thread/startVirtualThread
                (fn []
                  (while @running?
                    (try
                      (Thread/sleep (* heartbeat-seconds 1000))
                      (sweep-stale-leases! sweep-config)
                      (catch InterruptedException _
                        (reset! running? false))
                      (catch Throwable _
                        nil)))))]
    {:running? running?
     :thread thread}))

(defn- stop-sweeper!
  [{:keys [running? thread]}]
  (when running?
    (reset! running? false))
  (when thread
    (.interrupt ^Thread thread)))

(defn- merge-path-overrides
  [overrides]
  (let [base (paths/xdg-paths)
        merged (merge base overrides)]
    (cond-> merged
      (and (:runtime-dir overrides) (not (:socket-path overrides)))
      (assoc :socket-path (paths/path-str (:runtime-dir merged) "broker.sock"))

      (and (:runtime-dir overrides) (not (:lock-path overrides)))
      (assoc :lock-path (paths/path-str (:runtime-dir merged) "broker.lock")))))

(defn broker-system
  "Build the broker's donut.system definition.

  Options:
  - `:paths` overrides XDG-derived paths.
  - `:config` overrides file-derived broker config.
  - `:http` overrides `[:http]` config for tests/dev, e.g. `{:transport :tcp :port 0}`.
  - `:matrix-gateway` injects a fake gateway for tests."
  ([] (broker-system {}))
  ([{:keys [paths config http matrix-gateway]}]
   {::ds/defs
    {:broker
     {:paths #::ds{:start (fn [_]
                           (paths/ensure-runtime-dirs!
                            (merge-path-overrides paths)))}
      :process-lock #::ds{:start (fn [{::ds/keys [config]}]
                                  (start-process-lock! config))
                         :stop (fn [{::ds/keys [instance]}]
                                 (stop-process-lock! instance))
                         :config {:paths (ds/ref [:broker :paths])}}
      :config #::ds{:start (fn [{::ds/keys [config]}]
                            (config/deep-merge
                             (config/load-config (:paths config))
                             (:overrides config)
                             (when (:http config)
                               {:http (:http config)})))
                    :config {:paths (ds/ref [:broker :paths])
                             :overrides config
                             :http http}}
      :state #::ds{:start (fn [_] (atom (state/empty-state)))}
      :subscribers #::ds{:start (fn [_] (events/subscriber-store))}
      :matrix-gateway #::ds{:start (fn [{::ds/keys [config]}]
                                   (let [event-sink {:publish! #(events/publish!
                                                                 {:state* (:state* config)
                                                                  :subscribers* (:subscribers* config)}
                                                                 %)}
                                         gateway (or (:matrix-gateway config)
                                                     (matrix/gateway (:broker-config config)
                                                                     (:paths config)
                                                                     event-sink))]
                                     (matrix/start! gateway)))
                           :stop (fn [{::ds/keys [instance]}]
                                   (matrix/stop! instance))
                           :config {:matrix-gateway matrix-gateway
                                    :process-lock (ds/ref [:broker :process-lock])
                                    :broker-config (ds/ref [:broker :config])
                                    :paths (ds/ref [:broker :paths])
                                    :state* (ds/ref [:broker :state])
                                    :subscribers* (ds/ref [:broker :subscribers])}}
      :app #::ds{:start (fn [{::ds/keys [config]}]
                          (api/app {:state* (:state* config)
                                    :subscribers* (:subscribers* config)
                                    :matrix-gateway (:matrix-gateway config)
                                    :config (:broker-config config)}))
                 :config {:state* (ds/ref [:broker :state])
                          :subscribers* (ds/ref [:broker :subscribers])
                          :matrix-gateway (ds/ref [:broker :matrix-gateway])
                          :broker-config (ds/ref [:broker :config])}}
      :http-server #::ds{:start (fn [{::ds/keys [instance config]}]
                                  (or instance
                                      (http/start-server! (:handler config)
                                                          (get (:broker-config config) :http)
                                                          (:paths config))))
                         :stop (fn [{::ds/keys [instance]}]
                                 (http/stop-server! instance))
                         :config {:handler (ds/ref [:broker :app])
                                  :broker-config (ds/ref [:broker :config])
                                  :paths (ds/ref [:broker :paths])}}
      :lease-sweeper #::ds{:start (fn [{::ds/keys [config]}]
                                   (start-sweeper! config))
                          :stop (fn [{::ds/keys [instance]}]
                                  (stop-sweeper! instance))
                          :config {:state* (ds/ref [:broker :state])
                                   :matrix-gateway (ds/ref [:broker :matrix-gateway])
                                   :config (ds/ref [:broker :config])}}}}}))

(defn start!
  ([] (start! {}))
  ([opts]
   (try
     (ds/signal (broker-system opts) ::ds/start)
     (catch clojure.lang.ExceptionInfo ex
       (if-let [cause (ex-cause ex)]
         (if (:code (ex-data cause))
           (throw cause)
           (throw ex))
         (throw ex))))))

(defn stop!
  [system]
  (ds/signal system ::ds/stop))
