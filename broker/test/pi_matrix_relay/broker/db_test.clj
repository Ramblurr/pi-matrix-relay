(ns pi-matrix-relay.broker.db-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [donut.system :as ds]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.system :as system]
            [pi-matrix-relay.broker.test-util :as tu])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn temp-dir
  [prefix]
  (str (Files/createTempDirectory prefix (make-array FileAttribute 0))))

(defn schema-idents
  [conn]
  (set (map first (d/q '[:find ?ident
                         :where [_ :db/ident ?ident]]
                       @conn))))

(defn attr-schema
  [conn ident]
  (let [schema (d/pull @conn [:db/valueType :db/cardinality :db/unique :db/index]
                       [:db/ident ident])]
    [(:db/valueType schema)
     (:db/cardinality schema)
     (:db/unique schema)
     (boolean (:db/index schema))]))

(defn test-paths
  [state-dir]
  {:state-dir state-dir
   :broker-db-store-id (random-uuid)})

(deftest datahike-config-disables-history-for-broker-domain-state
  (testing "broker lease churn should not accumulate Datahike history by default"
    (is (= false (:keep-history? (db/datahike-config {:state-dir "/tmp/pi-matrix-relay-test"}))))))

(deftest start-conn-stores-sqlite-in-state-dir-and-applies-schema
  (testing "broker durable state uses a Datahike SQLite database in the broker state dir"
    (let [state-dir (temp-dir "pi-matrix-relay-db-test-state")
          conn (db/start-conn! {:paths (test-paths state-dir)})]
      (try
        (is (.exists (io/file state-dir "broker.sqlite")))
        (is (= {:project/id [:db.type/uuid :db.cardinality/one :db.unique/identity false]
                :project/key [:db.type/string :db.cardinality/one :db.unique/value true]
                :client/instance-id [:db.type/string :db.cardinality/one :db.unique/identity false]
                :client/subscribed-room-id [:db.type/string :db.cardinality/many nil true]
                :lease/id [:db.type/uuid :db.cardinality/one :db.unique/identity false]
                :lease/state [:db.type/keyword :db.cardinality/one nil true]
                :slot-room/project [:db.type/ref :db.cardinality/one nil true]
                :slot-room/room-id [:db.type/string :db.cardinality/one :db.unique/identity false]
                :request/id [:db.type/string :db.cardinality/one :db.unique/identity false]
                :request/status [:db.type/keyword :db.cardinality/one nil true]}
               {:project/id (attr-schema conn :project/id)
                :project/key (attr-schema conn :project/key)
                :client/instance-id (attr-schema conn :client/instance-id)
                :client/subscribed-room-id (attr-schema conn :client/subscribed-room-id)
                :lease/id (attr-schema conn :lease/id)
                :lease/state (attr-schema conn :lease/state)
                :slot-room/project (attr-schema conn :slot-room/project)
                :slot-room/room-id (attr-schema conn :slot-room/room-id)
                :request/id (attr-schema conn :request/id)
                :request/status (attr-schema conn :request/status)}))
        (is (every? (schema-idents conn)
                    [:project/id
                     :project/key
                     :client/instance-id
                     :client/subscribed-room-id
                     :lease/id
                     :lease/state
                     :slot-room/project
                     :slot-room/slot
                     :slot-room/room-id
                     :slot-room/name
                     :request/id
                     :request/fingerprint
                     :request/result-json]))
        (finally
          (db/release-conn! conn))))))

(deftest wrap-db-adds-current-db-snapshot-to-request
  (testing "Ring handlers receive an immutable Datahike db value, not the mutable conn"
    (let [snapshot {:snapshot true}
          seen* (atom nil)
          handler (fn [request]
                    (reset! seen* request)
                    {:status 204})
          response ((db/wrap-db handler (atom snapshot)) {:uri "/v1/health"})]
      (is (= {:status 204} response))
      (is (= snapshot (:app/db @seen*))))))

(deftest broker-system-starts-datahike-connection
  (testing "donut starts the durable broker db component after paths are resolved"
    (let [runtime-dir (temp-dir "pi-matrix-relay-db-test-runtime")
          state-dir (temp-dir "pi-matrix-relay-db-test-system-state")
          running (system/start! {:paths {:runtime-dir runtime-dir
                                          :state-dir state-dir
                                          :broker-db-store-id (random-uuid)}
                                  :http {:transport :tcp :port 0}
                                  :matrix-gateway (tu/fake-gateway)})]
      (try
        (let [conn (ds/instance running [:broker :db-conn])]
          (is (some? conn))
          (is (.exists (io/file state-dir "broker.sqlite")))
          (is (contains? (schema-idents conn) :lease/id)))
        (finally
          (system/stop! running))))))
