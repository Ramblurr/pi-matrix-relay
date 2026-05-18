(ns pi-matrix-relay.config
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
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
      :config-path (.join path config-dir "config.edn")
      :token-path (.join path config-dir "token")}))
  ([env]
   (let [config-home (or (env-value env "XDG_CONFIG_HOME")
                         (.join path (home-dir env) ".config"))
         config-dir (.join path config-home app-name)]
     {:config-dir config-dir
      :config-path (.join path config-dir "config.edn")
      :token-path (.join path config-dir "token")})))

(defn project-config-path
  "Return the project-local EDN config path below the exact Pi `ctx.cwd`."
  [cwd]
  (.join path cwd ".agents" "matrix-relay" "config.edn"))

(defn project-id
  [cwd]
  (.basename path cwd))

(def default-room-prompt-mode "mentions")
(def default-delivery-mode "follow-up")
(def allowed-room-prompt-modes #{"all" "mentions" "commands-only"})

(defn normalize-prompt-mode
  [mode]
  (let [mode (some-> mode str str/trim str/lower-case)]
    (case mode
      nil nil
      "" nil
      "mention" "mentions"
      mode)))

(defn valid-room-prompt-mode?
  [mode]
  (contains? allowed-room-prompt-modes (normalize-prompt-mode mode)))

(defn- nonblank
  [value]
  (when-not (str/blank? (str value))
    value))

(defn derive-local-alias
  [room-result explicit-alias cwd]
  (or (nonblank explicit-alias)
      (nonblank (:room/canonical-alias room-result))
      (nonblank (:room/name room-result))
      (project-id cwd)))

(defn room-binding
  ([room-result explicit-alias cwd]
   (room-binding room-result explicit-alias cwd nil))
  ([room-result explicit-alias cwd explicit-mode]
   (let [alias (derive-local-alias room-result explicit-alias cwd)
         mode (or (normalize-prompt-mode explicit-mode) default-room-prompt-mode)]
     (cond-> {:alias alias
              :room/id (:room/id room-result)
              :mode mode}
       (:room/canonical-alias room-result) (assoc :room/canonical-alias (:room/canonical-alias room-result))
       (:room/name room-result) (assoc :room/name (:room/name room-result))))))

(defn bind-room
  "Return project config with `room-result` saved under a local alias."
  ([project-config room-result explicit-alias cwd]
   (bind-room project-config room-result explicit-alias cwd nil))
  ([project-config room-result explicit-alias cwd explicit-mode]
   (let [binding (room-binding room-result explicit-alias cwd explicit-mode)]
     (assoc-in (or project-config {}) [:rooms (:alias binding)] binding))))

(defn resolve-target
  "Resolve a send target by local alias or by a room id already present in config."
  [project-config target]
  (let [rooms (:rooms project-config)]
    (or (get rooms target)
        (get rooms (keyword target))
        (some (fn [[_ binding]]
                (when (= target (:room/id binding))
                  binding))
              rooms))))

(defn ensure-parent-dir!
  [file-path]
  (.mkdirSync fs (.dirname path file-path) #js {:recursive true}))


(defn read-edn-file!
  [file-path]
  (when (.existsSync fs file-path)
    (let [text (.readFileSync fs file-path "utf8")]
      (when-not (str/blank? text)
        (reader/read-string text)))))

(defn write-edn-file!
  [file-path data]
  (ensure-parent-dir! file-path)
  (.writeFileSync fs file-path (str (pr-str data) "\n") "utf8")
  data)

(defn read-project-config!
  [cwd]
  (or (read-edn-file! (project-config-path cwd)) {}))

(defn write-project-config!
  [cwd config]
  (write-edn-file! (project-config-path cwd) config))
