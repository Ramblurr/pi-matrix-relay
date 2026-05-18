(ns pi-matrix-relay.broker.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [pi-matrix-relay.broker.paths :as paths]))

(def default-config
  {:matrix {:encrypted? true
            :operators []
            :device-name "pi-matrix-relay-broker"
            :space {:enabled? false}}
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
      (let [text (slurp file)]
        (when-not (str/blank? text)
          (edn/read-string text))))))

(defn read-token-file
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (not-empty (str/trim (slurp file))))))

(def space-modes #{:existing :create})

(defn- normalize-space-mode
  [mode]
  (cond
    (keyword? mode) mode
    (string? mode) (keyword mode)
    :else mode))

(defn- nonblank
  [value]
  (let [trimmed (str/trim (str value))]
    (when-not (str/blank? trimmed)
      trimmed)))

(defn- normalize-space-config
  [space]
  (let [space (or space {})
        mode (or (normalize-space-mode (:mode space))
                 (cond
                   (nonblank (:room-id-or-alias space)) :existing
                   (:enabled? space) :create))]
    (if (and (not (false? (:enabled? space))) (contains? space-modes mode))
      (cond-> (assoc space :enabled? true :mode mode)
        (= mode :create) (update :name #(or (nonblank %) "pi-matrix-relay")))
      {:enabled? false})))

(defn normalize-config
  [config token]
  (let [merged (deep-merge default-config config)
        normalized (assoc-in merged [:matrix :space]
                             (normalize-space-config (get-in config [:matrix :space])))]
    (cond-> normalized
      token (assoc-in [:matrix :access-token] token))))

(defn load-config
  ([]
   (load-config (paths/xdg-paths)))
  ([paths]
   (normalize-config
    (read-config-file (:config-path paths))
    (read-token-file (:token-path paths)))))
