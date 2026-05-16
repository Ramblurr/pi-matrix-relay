(ns pi-matrix-relay.broker.system
  (:require [donut.system :as ds]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.config :as config]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.http :as http]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.paths :as paths]
            [pi-matrix-relay.broker.state :as state]))

(defn- start-sweeper!
  [{:keys [state* config]}]
  (let [running? (atom true)
        heartbeat-seconds (get-in config [:leases :heartbeat-seconds] 30)
        stale-after-missed (get-in config [:leases :stale-after-missed] 3)
        thread (Thread/startVirtualThread
                (fn []
                  (while @running?
                    (try
                      (Thread/sleep (* heartbeat-seconds 1000))
                      (state/mark-stale-leases! state*
                                                (System/currentTimeMillis)
                                                heartbeat-seconds
                                                stale-after-missed)
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
                            (merge (paths/xdg-paths) paths)))}
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
                                   :config (ds/ref [:broker :config])}}}}}))

(defn start!
  ([] (start! {}))
  ([opts]
   (ds/signal (broker-system opts) ::ds/start)))

(defn stop!
  [system]
  (ds/signal system ::ds/stop))
