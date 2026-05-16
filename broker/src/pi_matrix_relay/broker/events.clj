(ns pi-matrix-relay.broker.events
  (:require [org.httpkit.server :as hk]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.state :as state]))

(defn format-sse
  [{:keys [id event data]}]
  (str "id: " id "\n"
       "event: " (or event "message") "\n"
       "data: " (json/write-json data) "\n\n"))

(defn subscriber-store
  []
  (atom {}))

(defn subscribe!
  [subscribers* client-id channel]
  (swap! subscribers* update client-id (fnil conj #{}) channel)
  nil)

(defn unsubscribe!
  [subscribers* client-id channel]
  (swap! subscribers* update client-id disj channel)
  nil)

(defn deliver-event!
  [subscribers* client-id event]
  (doseq [channel (get @subscribers* client-id)]
    (hk/send! channel (format-sse event) false)))

(defn publish!
  [{:keys [state* subscribers*]} event]
  (let [event (state/append-event! state* event)
        room-id (get-in event [:data :room :roomId])]
    (doseq [[client-id client] (:clients @state*)]
      (when (or (nil? room-id)
                (contains? (:subscriptions client) room-id))
        (deliver-event! subscribers* client-id event)))
    event))
