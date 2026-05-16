(ns pi-matrix-relay.broker.test-util
  (:require [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.state :as state]))

(defrecord FakeGateway [calls*]
  matrix/MatrixGateway
  (start! [this] this)
  (stop! [_] (swap! calls* conj [:stop]) nil)
  (health [_]
    {:status "ok" :matrix {:connected true :userId "@bot:example.org" :encrypted true}})
  (resolve-room! [_ room]
    (swap! calls* conj [:resolve-room room])
    {:roomId room :canonicalAlias (when (.startsWith (str room) "#") room) :name room})
  (create-room! [_ request]
    (swap! calls* conj [:create-room request])
    {:roomId (str "!" (:name request) ":example.org") :name (:name request)})
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
  []
  (->FakeGateway (atom [])))

(defn calls
  [gateway]
  @(:calls* gateway))

(defn test-env
  ([] (test-env (fake-gateway)))
  ([gateway]
   {:state* (atom (state/empty-state))
    :subscribers* (events/subscriber-store)
    :matrix-gateway gateway
    :config {:leases {:heartbeat-seconds 30}
             :matrix {:operators ["@operator:example.org"]}}}))

(defn request
  ([app method uri]
   (request app method uri nil))
  ([app method uri body]
   (let [body-stream (when body
                       (java.io.ByteArrayInputStream.
                        (.getBytes (json/write-json body) "UTF-8")))]
     (app (cond-> {:request-method method
                   :uri uri
                   :headers {}}
            body-stream (assoc :body body-stream))))))

(defn response-json
  [response]
  (json/read-json (:body response)))

(defn json-request
  [app method uri body]
  (response-json (request app method uri body)))
