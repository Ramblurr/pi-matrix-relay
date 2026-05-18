(ns pi-matrix-relay.broker.api.presenters
  (:import [java.text SimpleDateFormat]
           [java.util Date TimeZone]))

(defn- delivery-mode-name
  [mode]
  (when mode
    (name mode)))

(defn- iso-instant
  [ms]
  (when ms
    (let [fmt (doto (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                (.setTimeZone (TimeZone/getTimeZone "UTC")))]
      (.format fmt (Date. (long ms))))))

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

(defn room-delivery-mode
  [{:keys [room-id default-delivery-mode updated-at updated-by-client updated-by-user]}]
  {:roomId room-id
   :defaultDeliveryMode (delivery-mode-name default-delivery-mode)
   :updatedAt (iso-instant updated-at)
   :updatedByClient updated-by-client
   :updatedByUser updated-by-user})
