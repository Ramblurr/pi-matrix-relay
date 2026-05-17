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
    :db/unique :db.unique/value
    :db/index true}
   {:db/ident :project/root
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :project/display-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :client/instance-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :client/protocol-version
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :client/project
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :client/metadata-json
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :client/registered-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :client/last-heartbeat-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :client/state
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :client/subscribed-room-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/index true}

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
    :db/cardinality :db.cardinality/one}

   {:db/ident :lease/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :lease/project
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/client
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/slot
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/state
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/reservation-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/room
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/reserved-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :lease/acquired-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :lease/last-heartbeat-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :lease/suspect-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :lease/released-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :lease/release-reason
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident :joined-room/room-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :joined-room/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :joined-room/canonical-alias
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :joined-room/membership
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :joined-room/updated-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident :request/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :request/fingerprint
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/operation
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :request/owner-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/result-json
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/error-code
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/completed-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :request/expires-at
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
           :id (or (:broker-db-store-id path-config) store-id)
           :sqlite-opts {:pool-size 4}}
   :schema-flexibility :read
   :keep-history? false})

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
