(ns pi-matrix-relay.broker.db
  (:require [datahike-sqlite.core]
            [datahike.api :as d]
            [pi-matrix-relay.broker.paths :as paths]))

(def store-id #uuid "7d6ad9da-5f96-4ee8-8374-d7ce5248e4df")

(def schema
  [{:db/ident :project/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :project/key
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :project/root
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :project/display-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :slot-room/project
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :slot-room/slot
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :slot-room/room-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :slot-room/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :slot-room/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :slot-room/updated-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}])

(defn db-path
  [path-config]
  (or (:broker-db-path path-config)
      (paths/path-str (:state-dir path-config) "broker.sqlite")))

(defn datahike-config
  [path-config]
  {:store {:backend :sqlite
           :dbname (db-path path-config)
           :id store-id
           :sqlite-opts {:pool-size 4}}
   :schema-flexibility :read
   :keep-history? true})

(defn ensure-database!
  [config]
  (when-not (d/database-exists? config)
    (d/create-database config))
  config)

(defn apply-schema!
  [conn]
  (d/transact conn schema)
  conn)

(defn start-conn!
  [{:keys [paths]}]
  (paths/ensure-dir! (:state-dir paths))
  (let [config (ensure-database! (datahike-config paths))
        conn (d/connect config)]
    (apply-schema! conn)))

(defn release-conn!
  [conn]
  (when conn
    (d/release conn))
  nil)

(defn wrap-db
  [handler conn]
  (if conn
    (fn [request]
      (handler (assoc request :app/db @conn)))
    handler))
