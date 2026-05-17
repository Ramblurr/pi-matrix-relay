(ns pi-matrix-relay.broker.test-util
  (:require [clojure.string :as str]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.runtime :as runtime])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn temp-dir
  [prefix]
  (str (Files/createTempDirectory prefix (make-array FileAttribute 0))))

(defn test-db-conn
  []
  (db/start-conn! {:paths {:state-dir (temp-dir "pi-matrix-relay-test-state")
                           :broker-db-store-id (random-uuid)}}))

(defrecord FakeGateway [calls* rooms on-create-room]
  matrix/MatrixGateway
  (start! [this] this)
  (stop! [_] (swap! calls* conj [:stop]) nil)
  (health [_]
    {:status "ok" :matrix {:connected true :userId "@bot:example.org" :encrypted true}})
  (list-rooms! [_]
    (swap! calls* conj [:list-rooms])
    (or rooms []))
  (resolve-room! [_ room]
    (swap! calls* conj [:resolve-room room])
    {:roomId room :canonicalAlias (when (.startsWith (str room) "#") room) :name room :membership "join"})
  (create-room! [_ request]
    (swap! calls* conj [:create-room request])
    (when on-create-room
      (on-create-room request))
    {:roomId (str "!" (:name request) ":example.org") :name (:name request) :membership "join"})
  (ensure-users-power-level! [_ request]
    (swap! calls* conj [:ensure-users-power-level request])
    {:roomId (:roomId request)
     :users (:users request)
     :level (:level request)})
  (leave-room! [_ request]
    (swap! calls* conj [:leave-room request])
    {:roomId (:roomId request)
     :left true})
  (send-message! [_ request]
    (swap! calls* conj [:send-message request])
    {:roomId (get-in request [:target :roomId]) :eventId "$message:example.org"})
  (set-typing! [_ request]
    (swap! calls* conj [:typing request])
    {})
  (send-reaction! [_ request]
    (swap! calls* conj [:send-reaction request])
    {:roomId (:roomId request)
     :eventId "$reaction:example.org"
     :reactsToEventId (:eventId request)
     :key (:key request)})
  (send-file! [_ request]
    (swap! calls* conj [:send-file request])
    {:roomId (get-in request [:target :roomId]) :eventId "$file:example.org"})
  (download-media! [_ request]
    (swap! calls* conj [:download-media request])
    {:path "/tmp/downloaded"})
  (transcribe-media! [_ request]
    (swap! calls* conj [:transcribe-media request])
    {:transcript "hello"})
  (verification-start! [_ request]
    (swap! calls* conj [:verification-start request])
    {:verificationId "verification-1"})
  (verification-confirm! [_ verification-id]
    (swap! calls* conj [:verification-confirm verification-id])
    {:verificationId verification-id})
  (verification-cancel! [_ verification-id]
    (swap! calls* conj [:verification-cancel verification-id])
    {:verificationId verification-id})
  (verification-status [_]
    {:verifications []}))

(defn fake-gateway
  ([]
   (fake-gateway {}))
  ([{:keys [rooms on-create-room]}]
   (->FakeGateway (atom []) rooms on-create-room)))

(defn calls
  [gateway]
  @(:calls* gateway))

(defn test-env
  ([] (test-env (fake-gateway) (test-db-conn)))
  ([gateway] (test-env gateway (test-db-conn)))
  ([gateway conn]
   {:db-conn conn
    :runtime (runtime/create-runtime)
    :matrix-gateway gateway
    :config {:leases {:heartbeat-seconds 30}
             :matrix {:operators ["@operator:example.org"]}}}))

(defn request
  ([app method uri]
   (request app method uri nil))
  ([app method uri body]
   (let [[path query-string] (let [parts (str/split uri #"\?" 2)]
                               [(first parts) (second parts)])
         body-stream (when body
                       (java.io.ByteArrayInputStream.
                        (.getBytes (json/write-json body) "UTF-8")))]
     (app (cond-> {:request-method method
                   :uri path
                   :headers {}}
            query-string (assoc :query-string query-string)
            body-stream (assoc :body body-stream))))))

(defn response-json
  [response]
  (json/read-json (:body response)))

(defn json-request
  [app method uri body]
  (response-json (request app method uri body)))
