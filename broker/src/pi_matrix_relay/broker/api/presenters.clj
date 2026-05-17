(ns pi-matrix-relay.broker.api.presenters)

(defn client-registration
  [{:keys [client-id heartbeat-seconds global-operators]}]
  {:clientId client-id
   :eventStream (str "/v1/clients/" client-id "/events")
   :heartbeatSeconds heartbeat-seconds
   :globalOperators (vec global-operators)})

(defn subscriptions
  [{:keys [rooms]}]
  {:rooms (vec rooms)})

(defn heartbeat
  [heartbeat-seconds]
  {:heartbeatSeconds heartbeat-seconds})

(defn slot-acquire
  [{:keys [slot room-id room-name]}]
  {:slot slot
   :roomId room-id
   :roomName room-name})

(defn slot
  [{:keys [slot room-id room-name client-id client-metadata state acquired-at last-heartbeat-at]}]
  {:slot slot
   :roomId room-id
   :roomName room-name
   :clientId client-id
   :clientMetadata client-metadata
   :state (name state)
   :acquiredAt acquired-at
   :lastHeartbeatAt last-heartbeat-at})

(defn slots-list
  [{:keys [project-key slots]}]
  {:projectId project-key
   :slots (mapv slot slots)})
