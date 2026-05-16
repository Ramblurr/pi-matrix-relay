(ns pi-matrix-relay.broker.paths
  (:require [clojure.java.io :as io]
            [ol.dirs :as dirs])
  (:import [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]))

(def app-name "pi-matrix-relay")

(defn path-str
  [& parts]
  (str (apply io/file parts)))

(defn xdg-paths
  "Returns broker paths following XDG base directories via ol.dirs.

  The function does not create directories. Side effects belong in
  [[ensure-runtime-dirs!]]."
  []
  (let [config-dir (dirs/config-dir app-name)
        state-dir  (dirs/state-dir app-name)
        runtime-dir (or (dirs/runtime-dir app-name)
                        (path-str (System/getProperty "java.io.tmpdir") app-name))]
    {:config-dir config-dir
     :config-path (path-str config-dir "config.json")
     :token-path (path-str config-dir "token")
     :state-dir state-dir
     :crypto-dir (path-str state-dir "crypto")
     :media-dir (path-str state-dir "media")
     :database-path (path-str state-dir "trixnity.sqlite")
     :runtime-dir runtime-dir
     :socket-path (path-str runtime-dir "broker.sock")
     :lock-path (path-str runtime-dir "broker.lock")}))

(defn ensure-dir!
  [path]
  (Files/createDirectories
   (Path/of (str path) (make-array String 0))
   (make-array FileAttribute 0))
  path)

(defn ensure-runtime-dirs!
  [{:keys [config-dir state-dir crypto-dir media-dir runtime-dir] :as paths}]
  (doseq [dir [config-dir state-dir crypto-dir media-dir runtime-dir]]
    (ensure-dir! dir))
  paths)

(defn set-owner-read-write!
  "Set POSIX token-file permissions to 0600 when the filesystem supports it."
  [path]
  (let [p (Path/of (str path) (make-array String 0))]
    (try
      (Files/setPosixFilePermissions
       p
       (PosixFilePermissions/fromString "rw-------"))
      (catch UnsupportedOperationException _
        nil))
    path))
