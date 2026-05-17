(ns pi-matrix-relay.broker.events
  (:require [org.httpkit.server :as hk]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.runtime :as runtime]
            [pi-matrix-relay.broker.store :as store]))

(defn format-sse
  [{:keys [id event data]}]
  (str "id: " id "\n"
       "event: " (or event "message") "\n"
       "data: " (json/write-json data) "\n\n"))

(defn subscribe!
  [runtime client-id channel]
  (runtime/subscribe! runtime client-id channel)
  nil)

(defn unsubscribe!
  [runtime client-id channel]
  (runtime/unsubscribe! runtime client-id channel)
  nil)

(defn deliver-event!
  [runtime client-id event]
  (doseq [channel (runtime/subscriber-channels runtime client-id)]
    (hk/send! channel (format-sse event) false)))

(defn publish!
  [{:keys [db-conn runtime]} event]
  (let [event (runtime/append-event! runtime event)
        room-id (get-in event [:data :room :roomId])]
    (doseq [client-id (store/clients-for-room @db-conn room-id)]
      (deliver-event! runtime client-id event))
    event))
