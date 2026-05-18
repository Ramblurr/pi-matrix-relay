(ns pi-matrix-relay.config
  (:require [clojure.string :as str]
            [ol.dirs :as dirs]))

(def fs (js/require "fs"))
(def path (js/require "path"))
(def os (js/require "os"))

(def app-name "pi-matrix-relay")

(defn- env-value
  [env k]
  (aget (or env #js {}) k))

(defn- home-dir
  [env]
  (or (env-value env "HOME")
      (.homedir os)))

(defn global-paths
  "Return global Matrix relay config paths."
  ([]
   (let [config-dir (dirs/config-dir app-name)]
     {:config-dir config-dir
      :config-path (.join path config-dir "config.json")
      :token-path (.join path config-dir "token")}))
  ([env]
   (let [config-home (or (env-value env "XDG_CONFIG_HOME")
                         (.join path (home-dir env) ".config"))
         config-dir (.join path config-home app-name)]
     {:config-dir config-dir
      :config-path (.join path config-dir "config.json")
      :token-path (.join path config-dir "token")})))

(defn project-config-path
  "Return the project-local config path below the exact Pi `ctx.cwd`."
  [cwd]
  (.join path cwd ".agents" "matrix-relay" "config.json"))

(defn project-id
  [cwd]
  (.basename path cwd))

(def default-room-mode "mentions")
(def default-delivery-mode "follow-up")

(defn- nonblank
  [value]
  (when-not (str/blank? (str value))
    value))

(defn derive-local-alias
  [room-result explicit-alias cwd]
  (or (nonblank explicit-alias)
      (nonblank (:canonicalAlias room-result))
      (nonblank (:name room-result))
      (project-id cwd)))

(defn room-binding
  [room-result explicit-alias cwd]
  (let [alias (derive-local-alias room-result explicit-alias cwd)]
    (cond-> {:alias alias
             :roomId (:roomId room-result)
             :mode default-room-mode}
      (:canonicalAlias room-result) (assoc :canonicalAlias (:canonicalAlias room-result))
      (:name room-result) (assoc :name (:name room-result)))))

(defn bind-room
  "Return project config with `room-result` saved under a local alias."
  [project-config room-result explicit-alias cwd]
  (let [binding (room-binding room-result explicit-alias cwd)]
    (assoc-in (or project-config {}) [:rooms (:alias binding)] binding)))

(defn resolve-target
  "Resolve a send target by local alias or by a room id already present in config."
  [project-config target]
  (let [rooms (:rooms project-config)]
    (or (get rooms target)
        (get rooms (keyword target))
        (some (fn [[_ binding]]
                (when (= target (:roomId binding))
                  binding))
              rooms))))

(defn ensure-parent-dir!
  [file-path]
  (.mkdirSync fs (.dirname path file-path) #js {:recursive true}))

(defn read-json-file!
  [file-path]
  (when (.existsSync fs file-path)
    (let [text (.readFileSync fs file-path "utf8")]
      (when-not (str/blank? text)
        (js->clj (js/JSON.parse text) :keywordize-keys true)))))

(defn write-json-file!
  [file-path data]
  (ensure-parent-dir! file-path)
  (.writeFileSync fs file-path (str (js/JSON.stringify (clj->js data) nil 2) "\n") "utf8")
  data)

(defn read-project-config!
  [cwd]
  (or (read-json-file! (project-config-path cwd)) {}))

(defn write-project-config!
  [cwd config]
  (write-json-file! (project-config-path cwd) config))
