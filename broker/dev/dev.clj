(ns dev
  {:clj-kondo/config '{:linters       {:unused-namespace     {:level :off}
                                       :unresolved-namespace {:level :off}
                                       :unused-referred-var  {:level :off}}
                       :skip-comments true}}
  (:require
   [clj-reload.core :as clj-reload]
   [donut.system :as ds]
   [ol.dev.portal :as portal]
   [pi-matrix-relay.broker.matrix :as matrix]
   [pi-matrix-relay.broker.system :as broker.system]))

;; --------------------------------------------------------------------------------------------
;; Toggle Dev-time flags

(set! *print-namespace-maps* false)

;; --------------------------------------------------------------------------------------------
;; Portal & Logging

(defonce portal! (portal/open-portals))

(defn logs
  "Query debug log: (logs), (logs 5), (logs :label), (logs :label 3)."
  ([] (portal/logs))
  ([n-or-label] (portal/logs n-or-label))
  ([label n] (portal/logs label n)))

(defn log-values
  "Like logs, but returns just the values."
  ([] (portal/log-values))
  ([n-or-label] (portal/log-values n-or-label))
  ([label n] (portal/log-values label n)))

(defn clear-logs! [] (portal/clear-logs!))

(defn last-log
  "Most recent entry, or just value with (last-log :v)."
  ([] (portal/last-log))
  ([_] (portal/last-log :v)))

;; --------------------------------------------------------------------------------------------
;; System Control

(defonce broker-system* (atom nil))

;; `dev` is intentionally in clj-reload's :no-reload set so `broker-system*`
;; survives normal app reloads and direct namespace aliases are not re-evaluated
;; into stale/conflicting aliases. If you edit this namespace, reload it explicitly:
;;
;;   (remove-ns 'dev)
;;   (require '[dev :as dev])
;;
;; Or restart the REPL.

(defn system
  "Return the currently running broker donut system, or nil."
  []
  @broker-system*)

(defn instance
  "Return a component instance from the running broker system. Example:
  `(instance [:broker :http-server])`."
  [component-id]
  (when-let [running (system)]
    (ds/instance running component-id)))

(defn status
  "Return a compact summary of the REPL-managed broker system."
  []
  (let [http-server (instance [:broker :http-server])
        gateway (instance [:broker :matrix-gateway])]
    (cond-> {:running? (boolean (system))}
      http-server
      (assoc :http-server (select-keys http-server [:transport :socket-path :port]))

      gateway
      (assoc :matrix (try
                       (matrix/health gateway)
                       (catch Throwable ex
                         {:status "error"
                          :message (ex-message ex)}))))))

(defn start
  "Start the broker system from the REPL.

  Accepts the same opts as `pi-matrix-relay.broker.system/start!`, for example:
  `(start {:http {:transport :tcp :port 0}})`."
  ([]
   (start {}))
  ([opts]
   (if (system)
     (assoc (status) :status :already-running)
     (let [running (broker.system/start! opts)]
       (reset! broker-system* running)
       (assoc (status) :status :started)))))

(defn stop
  "Stop the REPL-managed broker system if it is running."
  []
  (if-let [running (system)]
    (try
      (broker.system/stop! running)
      {:status :stopped}
      (finally
        (reset! broker-system* nil)))
    {:status :not-running}))

(defn restart
  "Stop and start the broker system. Accepts the same opts as `start`."
  ([]
   (restart {}))
  ([opts]
   (stop)
   (start opts)))

(defn reset
  "Stop, reload changed namespaces, and start the broker system."
  ([]
   (reset {}))
  ([opts]
   (stop)
   (clj-reload/reload)
   (start opts)))

(defn reload-all []
  (clj-reload/reload {:only :all}))

;; --------------------------------------------------------------------------------------------
;; Code Reloading

(clj-reload/init {:dirs      ["src" "dev" "test"]
                  :no-reload '#{dev user ol.dev.portal}})

(comment
  ;; normal reload
  (reset) ;; rcf

  ;; when the above fails
  (do
    (clj-reload/reload {:only :loaded})
    (reset))

  ;; other
  (clj-reload/reload)
  (clj-reload/reload {:only :loaded})
  (stop)
  (reset)
  (restart)
  (reload-all)
  ;; Reload classpath when deps.edn changes
  (clojure.repl.deps/sync-deps)
  ;;
  )

(comment
  (tap> 2)
  (first @(portal/my-taps))
  (logs 5)
  (last-log))
