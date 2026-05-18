(ns pi-matrix-relay.broker.system
  (:require [donut.system :as ds]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.config :as config]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.http :as http]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.paths :as paths]
            [pi-matrix-relay.broker.runtime :as runtime]
            [pi-matrix-relay.broker.store :as store])
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
       (Instant/ofEpochMilli (:last-heartbeat-at lease))
       ". Slot lease released."))

(defn sweep-stale-leases!
  [{:keys [db-conn config matrix-gateway now runtime]}]
  (let [heartbeat-seconds (get-in config [:leases :heartbeat-seconds] 30)
        stale-after-missed (get-in config [:leases :stale-after-missed] 3)
        active-stream-clients (when runtime
                                (runtime/subscribed-client-ids runtime))
        stale (store/mark-stale-leases! db-conn
                                        (or now (System/currentTimeMillis))
                                        heartbeat-seconds
                                        stale-after-missed
                                        {:exclude-client-ids active-stream-clients})]
    (doseq [lease stale]
      (try
        (matrix/send-message! matrix-gateway
                              {:target {:room/id (:room-id lease)}
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

(defn- simple-health-value
  [value]
  (cond
    (or (nil? value) (string? value) (number? value) (keyword? value) (boolean? value)) value
    (map? value) (into {}
                       (map (fn [[k v]]
                              [(simple-health-value k) (simple-health-value v)]))
                       value)
    (set? value) (set (map simple-health-value value))
    (sequential? value) (mapv simple-health-value value)
    :else (str value)))

(defn- throwable-details
  [throwable]
  (not-empty (simple-health-value (dissoc (ex-data throwable) :code))))

(defn- matrix-space-success-status
  [result]
  (cond
    (false? (:space/enabled? result))
    {:status "disabled"
     :space/enabled? false}

    (:space/id result)
    (assoc result
           :status "ok"
           :space/enabled? true)

    :else
    (assoc (or result {})
           :status "ok"
           :space/enabled? true)))

(defn- matrix-space-error-status
  [throwable]
  {:status "error"
   :space/enabled? true
   :error (cond-> {:code (name (or (:code (ex-data throwable)) :matrix_space_setup_failed))
                   :message (or (ex-message throwable) "Matrix Space setup failed.")}
            (throwable-details throwable)
            (assoc :details (throwable-details throwable))

            (ex-cause throwable)
            (assoc-in [:details :cause]
                      {:message (or (ex-message (ex-cause throwable)) "unknown cause")
                       :class (.getName (class (ex-cause throwable)))}))})

(defn- ensure-matrix-space-status!
  [{:keys [matrix-gateway db-conn]}]
  (try
    (matrix-space-success-status
     (matrix/ensure-space! matrix-gateway {:db-conn db-conn}))
    (catch Throwable throwable
      (matrix-space-error-status throwable))))

(defn- slot-room-link-result
  [matrix-gateway slot-room]
  (try
    (assoc (matrix/ensure-room-in-space! matrix-gateway {:room/id (:room-id slot-room)})
           :project-key (:project-key slot-room)
           :slot (:slot slot-room))
    (catch Throwable throwable
      {:room/id (:room-id slot-room)
       :project-key (:project-key slot-room)
       :slot (:slot slot-room)
       :linked? false
       :error (:error (matrix-space-error-status throwable))})))

(defn- link-known-slot-rooms!
  [{:keys [db-conn matrix-gateway]}]
  (if (and db-conn matrix-gateway)
    (mapv #(slot-room-link-result matrix-gateway %)
          (store/list-slot-rooms @db-conn))
    []))

(defn- slot-room-link-summary
  [results]
  {:attempted (count results)
   :linked (count (filter :linked? results))
   :failed (vec (remove :linked? results))})

(defn reconcile-matrix-space!
  "Retry configured Matrix Space setup and, once available, link known slot rooms.

  This is safe to call periodically. It updates `:matrix-space`, an atom holding
  the health-visible Matrix Space status, and returns the new status map."
  [{:keys [matrix-space link-slot-rooms?] :as config}]
  (let [old-status (when matrix-space @matrix-space)]
    (if (= "disabled" (:status old-status))
      old-status
      (let [status (ensure-matrix-space-status! config)
            should-link? (and (= "ok" (:status status))
                              (or link-slot-rooms?
                                  (not= "ok" (:status old-status))))
            link-results (when should-link?
                           (link-known-slot-rooms! config))
            status (cond-> status
                     (seq link-results) (assoc :slot-room-links
                                               (slot-room-link-summary link-results)))]
        (when matrix-space
          (reset! matrix-space status))
        status))))

(defn- start-matrix-space-reconciler!
  [{:keys [config] :as reconcile-config}]
  (let [running? (atom true)
        first-run? (atom true)
        interval-ms (* 1000 (long (or (get-in config [:matrix :space :reconcile-seconds]) 10)))
        run-once! (fn []
                    (try
                      (reconcile-matrix-space! (assoc reconcile-config
                                                      :link-slot-rooms? @first-run?))
                      (reset! first-run? false)
                      (catch Throwable _
                        nil)))
        thread (Thread/startVirtualThread
                (fn []
                  (run-once!)
                  (while @running?
                    (try
                      (Thread/sleep interval-ms)
                      (run-once!)
                      (catch InterruptedException _
                        (reset! running? false))))))]
    {:running? running?
     :thread thread
     :interval-ms interval-ms}))

(defn- stop-matrix-space-reconciler!
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
      (assoc :lock-path (paths/path-str (:runtime-dir merged) "broker.lock"))

      (and (:state-dir overrides) (not (:crypto-dir overrides)))
      (assoc :crypto-dir (paths/path-str (:state-dir merged) "crypto"))

      (and (:state-dir overrides) (not (:media-dir overrides)))
      (assoc :media-dir (paths/path-str (:state-dir merged) "media"))

      (and (:state-dir overrides) (not (:database-path overrides)))
      (assoc :database-path (paths/path-str (:state-dir merged) "trixnity.sqlite"))

      (and (:state-dir overrides) (not (:broker-db-path overrides)))
      (assoc :broker-db-path (paths/path-str (:state-dir merged) "broker.sqlite")))))

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
      :runtime #::ds{:start (fn [_] (runtime/create-runtime))}
      :db-conn #::ds{:start (fn [{::ds/keys [config]}]
                              (db/start-conn! config))
                     :stop (fn [{::ds/keys [instance]}]
                             (db/release-conn! instance))
                     :config {:paths (ds/ref [:broker :paths])
                              :process-lock (ds/ref [:broker :process-lock])}}
      :matrix-gateway #::ds{:start (fn [{::ds/keys [config]}]
                                     (let [event-sink {:publish! #(events/publish!
                                                                   {:db-conn (:db-conn config)
                                                                    :runtime (:runtime config)}
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
                                     :db-conn (ds/ref [:broker :db-conn])
                                     :runtime (ds/ref [:broker :runtime])}}

      :matrix-space #::ds{:start (fn [{::ds/keys [config]}]
                                   (atom (ensure-matrix-space-status! {:matrix-gateway (:matrix-gateway config)
                                                                       :db-conn (:db-conn config)})))
                          :config {:matrix-gateway (ds/ref [:broker :matrix-gateway])
                                   :db-conn (ds/ref [:broker :db-conn])}}

      :matrix-space-reconciler #::ds{:start (fn [{::ds/keys [config]}]
                                              (start-matrix-space-reconciler! config))
                                     :stop (fn [{::ds/keys [instance]}]
                                             (stop-matrix-space-reconciler! instance))
                                     :config {:matrix-space (ds/ref [:broker :matrix-space])
                                              :matrix-gateway (ds/ref [:broker :matrix-gateway])
                                              :db-conn (ds/ref [:broker :db-conn])
                                              :config (ds/ref [:broker :config])}}
      :app #::ds{:start (fn [{::ds/keys [config]}]
                          (api/app {:db-conn (:db-conn config)
                                    :runtime (:runtime config)
                                    :matrix-gateway (:matrix-gateway config)
                                    :matrix-space (:matrix-space config)
                                    :config (:broker-config config)}))
                 :config {:db-conn (ds/ref [:broker :db-conn])
                          :runtime (ds/ref [:broker :runtime])
                          :matrix-gateway (ds/ref [:broker :matrix-gateway])
                          :broker-config (ds/ref [:broker :config])
                          :matrix-space (ds/ref [:broker :matrix-space])
                          :matrix-space-reconciler (ds/ref [:broker :matrix-space-reconciler])}}
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
                           :config {:db-conn (ds/ref [:broker :db-conn])
                                    :runtime (ds/ref [:broker :runtime])
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
