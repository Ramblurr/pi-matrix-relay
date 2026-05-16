(ns pi-matrix-relay.broker.config
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [pi-matrix-relay.broker.json :as broker.json]
            [pi-matrix-relay.broker.paths :as paths]))

(def default-config
  {:matrix {:encrypted? true
            :operators []
            :device-name "pi-matrix-relay-broker"}
   :http {:transport :uds}
   :leases {:heartbeat-seconds 30
            :suspect-after-missed 2
            :stale-after-missed 3}
   :events {:buffer-size 512}})

(defn deep-merge
  [& maps]
  (letfn [(merge-values [left right]
            (if (and (map? left) (map? right))
              (merge-with merge-values left right)
              right))]
    (reduce merge-values {} (remove nil? maps))))

(defn read-config-file
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (broker.json/read-json (slurp file)))))

(defn read-token-file
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (not-empty (str/trim (slurp file))))))

(defn normalize-config
  [config token]
  (cond-> (deep-merge default-config config)
    token (assoc-in [:matrix :access-token] token)))

(defn load-config
  ([]
   (load-config (paths/xdg-paths)))
  ([paths]
   (normalize-config
    (read-config-file (:config-path paths))
    (read-token-file (:token-path paths)))))
