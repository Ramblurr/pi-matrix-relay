(ns pi-matrix-relay.broker.main
  (:require [donut.system :as ds]
            [pi-matrix-relay.broker.system :as system]))

(set! *warn-on-reflection* true)

(defonce running-system* (atom nil))

(defn stop! []
  (when-let [system @running-system*]
    (reset! running-system* nil)
    (system/stop! system)))

(defn start!
  ([] (start! {}))
  ([opts]
   (let [running (system/start! opts)]
     (reset! running-system* running)
     running)))

(defn- parse-long-safe
  [s]
  (when s
    (Long/parseLong s)))

(defn parse-args
  [args]
  (loop [args args
         opts {}]
    (case (first args)
      nil opts
      "--tcp-port" (recur (nnext args)
                           (assoc opts :http {:transport :tcp
                                              :port (parse-long-safe (second args))}))
      "--uds" (recur (next args) (assoc opts :http {:transport :uds}))
      "--socket" (recur (nnext args)
                         (assoc-in opts [:paths :socket-path] (second args)))
      (throw (ex-info "Unknown broker argument." {:arg (first args)})))))

(defn -main
  [& args]
  (let [system (start! (parse-args args))
        http-server (ds/instance system [:broker :http-server])]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable stop! "pi-matrix-relay-broker-shutdown"))
    (println "pi-matrix-relay broker started"
             (select-keys http-server [:transport :socket-path :port]))
    @(promise)))
