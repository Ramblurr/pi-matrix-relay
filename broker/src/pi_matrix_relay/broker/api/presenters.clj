(ns pi-matrix-relay.broker.api.presenters
  (:import [java.text SimpleDateFormat]
           [java.util Date TimeZone]))

(defn- keyword-name
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
  {:client/id client-id
   :event-stream/path (str "/v1/clients/" client-id "/events")
   :heartbeat/seconds heartbeat-seconds
   :matrix/global-operators (vec global-operators)})

(defn subscriptions
  [{:keys [rooms]}]
  {:rooms (vec rooms)})

(defn heartbeat
  [heartbeat-seconds]
  {:heartbeat/seconds heartbeat-seconds})

(defn slot-acquire
  [{:keys [slot room-id room-name]}]
  {:slot slot
   :room/id room-id
   :room/name room-name})

(defn slot
  [{:keys [slot room-id room-name client-id client-metadata state acquired-at last-heartbeat-at]}]
  {:slot slot
   :room/id room-id
   :room/name room-name
   :client/id client-id
   :client/metadata client-metadata
   :state (name state)
   :lease/acquired-at acquired-at
   :lease/last-heartbeat-at last-heartbeat-at})

(defn slots-list
  [{:keys [project-key slots]}]
  {:project/id project-key
   :slots (mapv slot slots)})

(defn room-delivery-mode
  [{:keys [room-id default-delivery-mode updated-at updated-by-client updated-by-user]}]
  {:room/id room-id
   :room/default-delivery-mode (keyword-name default-delivery-mode)
   :room/default-delivery-mode-updated-at (iso-instant updated-at)
   :room/default-delivery-mode-updated-by-client updated-by-client
   :room/default-delivery-mode-updated-by-user updated-by-user})

(defn room-prompt-mode
  [{:keys [room-id mode updated-at updated-by-client updated-by-user]}]
  {:room/id room-id
   :room/prompt-mode (keyword-name mode)
   :room/prompt-mode-updated-at (iso-instant updated-at)
   :room/prompt-mode-updated-by-client updated-by-client
   :room/prompt-mode-updated-by-user updated-by-user})
