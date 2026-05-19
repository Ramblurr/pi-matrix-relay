(ns pi-matrix-relay.broker.test-util
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [pi-matrix-relay.broker.db :as db]
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

(defrecord FakeGateway [calls* rooms on-create-room space-id on-ensure-space]
  matrix/MatrixGateway
  (start! [this] this)
  (stop! [_] (swap! calls* conj [:stop]) nil)
  (health [_]
    {:status "ok" :matrix/connected? true :user/id "@bot:example.org" :matrix/encrypted? true})
  (list-rooms! [_]
    (swap! calls* conj [:list-rooms])
    (or rooms []))
  (resolve-room! [_ room]
    (swap! calls* conj [:resolve-room room])
    {:room/id room :room/canonical-alias (when (.startsWith (str room) "#") room) :room/name room :room/membership "join"})
  (create-room! [_ request]
    (swap! calls* conj [:create-room request])
    (when on-create-room
      (on-create-room request))
    {:room/id (str "!" (:room/name request) ":example.org") :room/name (:room/name request) :room/membership "join"})
  (ensure-users-power-level! [_ request]
    (swap! calls* conj [:ensure-users-power-level request])
    {:room/id (:room/id request)
     :users (:users request)
     :level (:level request)})
  (ensure-space! [_ request]
    (swap! calls* conj [:ensure-space request])
    (when on-ensure-space
      (on-ensure-space request))
    (if space-id
      {:space/id space-id :space/mode :existing}
      {:space/enabled? false}))
  (ensure-room-in-space! [_ request]
    (swap! calls* conj [:ensure-room-in-space request])
    (cond-> {:room/id (:room/id request)
             :linked? (boolean space-id)}
      space-id (assoc :space/id space-id)))
  (leave-room! [_ request]
    (swap! calls* conj [:leave-room request])
    {:room/id (:room/id request)
     :left true})
  (send-message! [_ request]
    (swap! calls* conj [:send-message request])
    {:room/id (get-in request [:target :room/id]) :event/id "$message:example.org"})
  (set-typing! [_ request]
    (swap! calls* conj [:typing request])
    {})
  (send-reaction! [_ request]
    (swap! calls* conj [:send-reaction request])
    {:room/id (:room/id request)
     :event/id "$reaction:example.org"
     :event/reacts-to-id (:event/id request)
     :key (:key request)})
  (send-file! [_ request]
    (swap! calls* conj [:send-file request])
    {:room/id (get-in request [:target :room/id]) :event/id "$file:example.org"})
  (download-media! [_ request]
    (swap! calls* conj [:download-media request])
    {:path "/tmp/downloaded"})
  (transcribe-media! [_ request]
    (swap! calls* conj [:transcribe-media request])
    {:transcript "hello"})
  (verification-start! [_ request]
    (swap! calls* conj [:verification-start request])
    {:verification-id "verification-1"})
  (verification-accept! [_ verification-id]
    (swap! calls* conj [:verification-accept verification-id])
    {:verification-id verification-id})
  (verification-start-sas! [_ verification-id]
    (swap! calls* conj [:verification-start-sas verification-id])
    {:verification-id verification-id})
  (verification-confirm! [_ verification-id]
    (swap! calls* conj [:verification-confirm verification-id])
    {:verification-id verification-id})
  (verification-no-match! [_ verification-id]
    (swap! calls* conj [:verification-no-match verification-id])
    {:verification-id verification-id})
  (verification-cancel! [_ verification-id]
    (swap! calls* conj [:verification-cancel verification-id])
    {:verification-id verification-id})
  (verification-status [_]
    {:verifications [{:verification-id "verification-1"}]}))

(defn fake-gateway
  ([]
   (fake-gateway {}))
  ([{:keys [rooms on-create-room space-id on-ensure-space]}]
   (->FakeGateway (atom []) rooms on-create-room space-id on-ensure-space)))

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
         body-stream (when (some? body)
                       (java.io.ByteArrayInputStream.
                        (.getBytes (pr-str body) "UTF-8")))]
     (app (cond-> {:request-method method
                   :uri path
                   :headers (cond-> {"accept" "application/edn"}
                              body-stream (assoc "content-type" "application/edn"))}
            query-string (assoc :query-string query-string)
            body-stream (assoc :body body-stream))))))

(defn response-edn
  [response]
  (let [body (:body response)]
    (cond
      (nil? body) nil
      (string? body) (edn/read-string body)
      :else (edn/read-string (slurp body)))))

(defn edn-request
  [app method uri body]
  (response-edn (request app method uri body)))
