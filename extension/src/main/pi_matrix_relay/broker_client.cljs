(ns pi-matrix-relay.broker-client
  (:require [clojure.string :as str]
            [ol.dirs :as dirs]
            [pi-matrix-relay.config :as config]))

(def http (js/require "http"))
(def path (js/require "path"))

(defn socket-path
  "Return the broker Unix domain socket path."
  ([]
   (or (dirs/runtime-dir config/app-name)
       (.join path "/tmp" config/app-name "broker.sock")))
  ([env]
   (.join path (or (aget (or env #js {}) "XDG_RUNTIME_DIR")
                   "/tmp")
          config/app-name
          "broker.sock")))

(defn request-options
  [env method uri body]
  (let [headers (cond-> {"Accept" "application/json"}
                  body (assoc "Content-Type" "application/json"
                              "Content-Length" (.-length (js/Buffer.from body "utf8"))))]
    (clj->js {:socketPath (socket-path env)
              :path uri
              :method method
              :headers headers})))

(defn encode-path-segment
  [segment]
  (js/encodeURIComponent (str segment)))

(defn- client-path
  ([client-id]
   (client-path client-id nil))
  ([client-id suffix]
   (str "/v1/clients/" (encode-path-segment client-id) suffix)))

(defn event-stream-options
  [env client-id]
  (clj->js {:socketPath (socket-path env)
            :path (client-path client-id "/events")
            :method "GET"
            :headers {"Accept" "text/event-stream"}}))

(defn broker-error
  [{:keys [message code details]}]
  (let [err (js/Error. (or message "Broker request failed"))]
    (set! (.-data err) (clj->js {:code code
                                 :details (or details {})}))
    err))

(defn unwrap-envelope
  [envelope]
  (if (:ok envelope)
    (:data envelope)
    (throw (broker-error (:error envelope)))))

(defn- parse-json
  [text]
  (when-not (str/blank? text)
    (js->clj (js/JSON.parse text) :keywordize-keys true)))

(defn- parse-sse-frame
  [frame]
  (let [lines (str/split-lines frame)
        parsed (reduce (fn [acc line]
                         (cond
                           (str/starts-with? line ":")
                           acc

                           (str/starts-with? line "id:")
                           (assoc acc :id (str/trim (subs line 3)))

                           (str/starts-with? line "event:")
                           (assoc acc :event (str/trim (subs line 6)))

                           (str/starts-with? line "data:")
                           (update acc :data-lines (fnil conj []) (str/triml (subs line 5)))

                           :else
                           acc))
                       {}
                       lines)]
    (when (seq (:data-lines parsed))
      {:id (:id parsed)
       :event (:event parsed)
       :data (parse-json (str/join "\n" (:data-lines parsed)))})))

(defn parse-sse-chunk
  [buffer chunk]
  (let [combined (str (or buffer "") chunk)
        parts (str/split combined #"\n\n" -1)
        frames (butlast parts)]
    {:events (->> frames
                  (keep parse-sse-frame)
                  vec)
     :buffer (last parts)}))

(defn request-json!
  ([method uri body]
   (request-json! {:env (.-env js/process)} method uri body))
  ([{:keys [env]} method uri body]
   (let [env (or env (.-env js/process))
         payload (when body (js/JSON.stringify (clj->js body)))]
     (js/Promise.
      (fn [resolve reject]
        (let [req (.request http
                            (request-options env method uri payload)
                            (fn [^js res]
                              (let [chunks (atom [])]
                                (.setEncoding res "utf8")
                                (.on res "data" #(swap! chunks conj %))
                                (.on res "end"
                                     (fn []
                                       (try
                                         (resolve (unwrap-envelope (parse-json (apply str @chunks))))
                                         (catch js/Error err
                                           (reject err))))))))]
          (.on req "error" reject)
          (when payload
            (.write req payload))
          (.end req)))))))

(defn health!
  ([]
   (health! {}))
  ([opts]
   (request-json! opts "GET" "/v1/health" nil)))

(defn register-client!
  ([request]
   (register-client! {} request))
  ([opts request]
   (request-json! opts "POST" "/v1/clients" request)))

(defn update-subscriptions!
  ([client-id rooms]
   (update-subscriptions! {} client-id rooms))
  ([opts client-id rooms]
   (request-json! opts "PATCH" (client-path client-id "/subscriptions")
                  {:rooms rooms})))

(defn- client-room-path
  [client-id room-id suffix]
  (str (client-path client-id "/rooms/")
       (encode-path-segment room-id)
       suffix))

(defn get-room-delivery-mode!
  ([client-id room-id]
   (get-room-delivery-mode! {} client-id room-id))
  ([opts client-id room-id]
   (request-json! opts "GET" (client-room-path client-id room-id "/delivery-mode") nil)))

(defn set-room-delivery-mode!
  ([client-id room-id default-delivery-mode updated-by-user]
   (set-room-delivery-mode! {} client-id room-id default-delivery-mode updated-by-user))
  ([opts client-id room-id default-delivery-mode updated-by-user]
   (request-json! opts "PUT" (client-room-path client-id room-id "/delivery-mode")
                  {:defaultDeliveryMode default-delivery-mode
                   :updatedByUser updated-by-user})))

(defn heartbeat!
  ([client-id]
   (heartbeat! {} client-id))
  ([opts client-id]
   (request-json! opts "POST" (client-path client-id "/heartbeat") {})))

(defn unregister-client!
  ([client-id reason]
   (unregister-client! {} client-id reason))
  ([opts client-id reason]
   (request-json! opts "DELETE" (client-path client-id) {:reason reason})))

(defn acquire-slot!
  ([client-id project invite]
   (acquire-slot! {} client-id project invite))
  ([opts client-id project invite]
   (request-json! opts "POST" "/v1/slots/acquire"
                  (cond-> {:clientId client-id
                           :project project}
                    (seq invite) (assoc :invite invite)))))

(defn release-slot!
  ([client-id room-id slot]
   (release-slot! {} client-id room-id slot))
  ([opts client-id room-id slot]
   (request-json! opts "POST" "/v1/slots/release"
                  {:clientId client-id
                   :roomId room-id
                   :slot slot})))

(defn list-slots!
  ([project-id]
   (list-slots! {} project-id))
  ([opts project-id]
   (request-json! opts "GET" (str "/v1/slots?projectId=" (encode-path-segment project-id)) nil)))

(defn list-rooms!
  ([]
   (list-rooms! {}))
  ([opts]
   (request-json! opts "GET" "/v1/matrix/rooms" nil)))

(defn- now-ms []
  (.getTime (js/Date.)))

(defn- error-data
  [err]
  (cond-> {:message (.-message err)}
    (.-code err) (assoc :code (.-code err))))

(defn open-event-stream!
  ([client-id on-event]
   (open-event-stream! {:env (.-env js/process)} client-id on-event))
  ([{:keys [env on-error]} client-id on-event]
   (let [env (or env (.-env js/process))
         buffer* (atom "")
         state* (atom {:clientId client-id
                       :startedAt (now-ms)
                       :connected false
                       :closed false
                       :chunkCount 0
                       :eventCount 0})
         record-error! (fn [err]
                         (swap! state* assoc
                                :lastError (error-data err)
                                :lastErrorAt (now-ms))
                         (when on-error
                           (on-error err)))
         req (.request http
                       (event-stream-options env client-id)
                       (fn [^js res]
                         (let [status-code (.-statusCode res)]
                           (swap! state* assoc
                                  :statusCode status-code
                                  :responseAt (now-ms)
                                  :connected (= 200 status-code))
                           (when (not= 200 status-code)
                             (record-error! (js/Error. (str "Event stream HTTP " status-code))))
                           (.setEncoding res "utf8")
                           (.on res "data"
                                (fn [chunk]
                                  (swap! state* #(-> %
                                                     (update :chunkCount (fnil inc 0))
                                                     (assoc :lastChunkAt (now-ms))))
                                  (try
                                    (let [{:keys [events buffer]} (parse-sse-chunk @buffer* chunk)]
                                      (reset! buffer* buffer)
                                      (doseq [event events]
                                        (swap! state* #(-> %
                                                           (update :eventCount (fnil inc 0))
                                                           (assoc :lastEventAt (now-ms)
                                                                  :lastEventId (:id event)
                                                                  :lastEventType (:event event))))
                                        (on-event (:data event))))
                                    (catch js/Error err
                                      (record-error! err)))))
                           (.on res "end"
                                (fn []
                                  (swap! state* assoc
                                         :closed true
                                         :closedAt (now-ms)
                                         :closeReason "response-end")))
                           (.on res "close"
                                (fn []
                                  (swap! state* assoc
                                         :closed true
                                         :closedAt (now-ms)
                                         :closeReason "response-close"))))))]
     (.on req "error" record-error!)
     (.on req "close" (fn []
                         (swap! state* assoc :requestClosedAt (now-ms))))
     (.end req)
     #js {:close (fn []
                   (swap! state* assoc
                          :closeRequested true
                          :closeRequestedAt (now-ms))
                   (.destroy req))
          :diagnostics (fn []
                         (clj->js @state*))})))

(defn resolve-room!
  ([room]
   (resolve-room! {} room))
  ([opts room]
   (request-json! opts "POST" "/v1/matrix/rooms/resolve" {:room room})))

(defn send-message!
  ([room-id message]
   (send-message! {} room-id message nil))
  ([opts-or-room-id room-id-or-message message-or-opts]
   (if (map? opts-or-room-id)
     (send-message! opts-or-room-id room-id-or-message message-or-opts nil)
     (send-message! {} opts-or-room-id room-id-or-message message-or-opts)))
  ([opts room-id message send-opts]
   (request-json! opts "POST" "/v1/matrix/messages"
                  (cond-> {:target {:roomId room-id}
                           :body message}
                    (:clientId send-opts)
                    (assoc :clientId (:clientId send-opts))

                    (:replyToEventId send-opts)
                    (assoc :replyTo {:roomId room-id
                                     :eventId (:replyToEventId send-opts)})))))

(defn send-reaction!
  ([room-id event-id key]
   (send-reaction! {} room-id event-id key nil))
  ([opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts]
   (if (map? opts-or-room-id)
     (send-reaction! opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts nil)
     (send-reaction! {} opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts)))
  ([opts room-id event-id key send-opts]
   (request-json! opts "POST" "/v1/matrix/reactions"
                  (cond-> {:roomId room-id
                           :eventId event-id
                           :key key}
                    (:clientId send-opts)
                    (assoc :clientId (:clientId send-opts))))))
