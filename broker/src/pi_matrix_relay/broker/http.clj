(ns pi-matrix-relay.broker.http
  (:require [clojure.java.io :as io]
            [org.httpkit.server :as hk]
            [pi-matrix-relay.broker.paths :as paths])
  (:import [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels ServerSocketChannel]
           [java.nio.file Files Path]))

(defn- delete-existing-socket!
  [socket-path]
  (let [path (Path/of (str socket-path) (make-array String 0))]
    (when (Files/exists path (make-array java.nio.file.LinkOption 0))
      (Files/delete path))))

(defn uds-server-options
  [socket-path]
  (when-let [parent (.getParentFile (io/file socket-path))]
    (paths/ensure-dir! (.getPath parent)))
  (delete-existing-socket! socket-path)
  {:legacy-return-value? false
   :address-finder (fn [] (UnixDomainSocketAddress/of (str socket-path)))
   :channel-factory (fn [_]
                      (ServerSocketChannel/open StandardProtocolFamily/UNIX))})

(defn tcp-server-options
  [{:keys [ip port]}]
  {:legacy-return-value? false
   :ip (or ip "127.0.0.1")
   :port (or port 0)})

(defn server-options
  [{:keys [transport socket-path] :as http-config} paths]
  (if (= :tcp transport)
    (tcp-server-options http-config)
    (uds-server-options (or socket-path (:socket-path paths)))))

(defn start-server!
  [handler http-config paths]
  (let [options (server-options http-config paths)
        server (hk/run-server handler options)]
    {:server server
     :options options
     :transport (if (= :tcp (:transport http-config)) :tcp :uds)
     :socket-path (when-not (= :tcp (:transport http-config))
                    (or (:socket-path http-config) (:socket-path paths)))
     :port (when (= :tcp (:transport http-config))
             (hk/server-port server))}))

(defn stop-server!
  [{:keys [server]}]
  (when server
    (hk/server-stop! server {:timeout 1000})))
