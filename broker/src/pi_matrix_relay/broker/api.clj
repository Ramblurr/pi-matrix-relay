(ns pi-matrix-relay.broker.api
  (:require [org.httpkit.server :as hk]
            [reitit.ring :as ring]
            [ring.middleware.params :refer [wrap-params]]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.slots :as slots]
            [pi-matrix-relay.broker.state :as state]))

(defn- body-params
  [request]
  (or (:json-params request)
      (when (:body request)
        (json/read-json (:body request)))
      {}))

(defn- ex-response
  [throwable]
  (let [data (ex-data throwable)
        code (or (:code data) :internal_error)
        status (case code
                 :invalid_request 400
                 :client_not_found 404
                 :room_not_allowed 403
                 :slot_not_found 404
                 :matrix_not_configured 503
                 :matrix_access_token_unsupported 501
                 :media_download_unavailable 501
                 :transcription_unavailable 501
                 :verification_unavailable 501
                 :slot_room_ambiguous 409
                 :matrix_http_failed 502
                 500)]
    (json/error-response status code (ex-message throwable) (dissoc data :code))))

(defn wrap-json-body
  [handler]
  (fn [request]
    (handler
     (if (and (:body request) (not (:json-params request)))
       (assoc request :json-params (body-params request))
       request))))

(defn wrap-errors
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch clojure.lang.ExceptionInfo ex
        (ex-response ex))
      (catch Throwable ex
        (json/error-response 500 :internal_error (or (ex-message ex) "Internal broker error") {})))))

(defn- client-id-param
  [request]
  (get-in request [:path-params :clientId]))

(defn- request-id
  [request]
  (:requestId (body-params request)))

(defn- idempotent!
  [{:keys [state*]} request thunk]
  (if-let [rid (request-id request)]
    (let [prior (get-in @state* [:sent-request-ids rid])]
      (if prior
        prior
        (let [result (thunk)]
          (swap! state* assoc-in [:sent-request-ids rid] result)
          result)))
    (thunk)))

(defn health-handler
  [{:keys [matrix-gateway]}]
  (fn [_]
    (json/ok-response (matrix/health matrix-gateway))))

(defn register-client-handler
  [{:keys [state* config] :as env}]
  (fn [request]
    (json/ok-response
     (idempotent!
      env request
      #(state/register-client!
        state*
        {:heartbeat-seconds (get-in config [:leases :heartbeat-seconds] 30)
         :global-operators (get-in config [:matrix :operators] [])}
        (body-params request))))))

(defn update-subscriptions-handler
  [{:keys [state*]}]
  (fn [request]
    (let [client-id (client-id-param request)
          body (body-params request)]
      (json/ok-response (state/update-subscriptions! state* client-id (:rooms body))))))

(defn heartbeat-handler
  [{:keys [state* config]}]
  (fn [request]
    (let [client-id (client-id-param request)
          now (System/currentTimeMillis)]
      (state/heartbeat! state* client-id now)
      (json/ok-response {:heartbeatSeconds (get-in config [:leases :heartbeat-seconds] 30)}))))

(defn unregister-client-handler
  [{:keys [state*]}]
  (fn [request]
    (json/ok-response (state/unregister-client! state* (client-id-param request)))))

(defn acks-handler
  [_]
  (fn [request]
    (json/ok-response {:accepted (count (:acks (body-params request)))})))

(defn event-stream-handler
  [{:keys [state* subscribers*]}]
  (fn [request]
    (let [client-id (client-id-param request)
          last-event-id (get-in request [:headers "last-event-id"])]
      (state/require-client @state* client-id)
      (hk/as-channel
       request
       {:on-open (fn [channel]
                   (hk/send! channel {:status 200
                                      :headers {"Content-Type" "text/event-stream"
                                                "Cache-Control" "no-cache"
                                                "Connection" "keep-alive"}
                                      :body ": connected\n\n"}
                             false)
                   (events/subscribe! subscribers* client-id channel)
                   (doseq [event (state/replay-events-after @state* last-event-id client-id)]
                     (hk/send! channel (events/format-sse event) false)))
        :on-close (fn [channel _]
                    (events/unsubscribe! subscribers* client-id channel)
                    (state/mark-client-suspect! state* client-id (System/currentTimeMillis)))}))))

(defn resolve-room-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          result (idempotent! env request #(matrix/resolve-room! matrix-gateway (:room body)))]
      (state/joined-room! state* result)
      (json/ok-response result))))

(defn create-room-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          result (idempotent! env request #(matrix/create-room! matrix-gateway body))]
      (state/joined-room! state* result)
      (json/ok-response result))))

(defn- joined-room?
  [room]
  (= "join" (:membership room)))

(defn list-rooms-handler
  [{:keys [matrix-gateway]}]
  (fn [_]
    (json/ok-response {:rooms (->> (matrix/list-rooms! matrix-gateway)
                                   (filter joined-room?)
                                   vec)})))

(defn ensure-send-authorized!
  [state client-id room-id]
  (when-not (if client-id
              (state/known-room-for-client? state client-id room-id)
              (boolean (state/joined-room state room-id)))
    (throw (ex-info (if client-id
                      "Client is not registered for the target Matrix room."
                      "Target Matrix room has not been joined or registered for this client.")
                    {:code :room_not_allowed
                     :client-id client-id
                     :room-id room-id}))))

(defn send-message-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (get-in body [:target :roomId])]
      (ensure-send-authorized! @state* client-id room-id)
      (json/ok-response
       (idempotent! env request #(matrix/send-message! matrix-gateway body))))))

(defn typing-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (:roomId body)]
      (ensure-send-authorized! @state* client-id room-id)
      (json/ok-response
       (idempotent! env request #(matrix/set-typing! matrix-gateway body))))))

(defn send-reaction-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (:roomId body)]
      (ensure-send-authorized! @state* client-id room-id)
      (json/ok-response
       (idempotent! env request #(matrix/send-reaction! matrix-gateway body))))))

(defn send-file-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (get-in body [:target :roomId])]
      (ensure-send-authorized! @state* client-id room-id)
      (json/ok-response
       (idempotent! env request #(matrix/send-file! matrix-gateway body))))))

(defn download-media-handler
  [{:keys [matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (idempotent! env request #(matrix/download-media! matrix-gateway body))))))

(defn transcribe-media-handler
  [{:keys [matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (idempotent! env request #(matrix/transcribe-media! matrix-gateway body))))))

(defn- room-display-name
  [room]
  (or (:name room) (:roomName room)))

(defn- remember-slot-room-from-matrix-room!
  [state* project-id slot room-name room]
  (state/remember-slot-room!
   state*
   project-id
   slot
   {:roomId (:roomId room)
    :name (or (room-display-name room) room-name)}))

(defn- matching-joined-slot-rooms
  [matrix-gateway room-name]
  (->> (matrix/list-rooms! matrix-gateway)
       (filter joined-room?)
       (filter #(= room-name (room-display-name %)))
       (sort-by :roomId)
       vec))

(defn- ensure-slot-room!
  [state* matrix-gateway project-id slot room-name invite]
  (let [slot-room (or (state/slot-room @state* project-id slot)
                      (let [matches (matching-joined-slot-rooms matrix-gateway room-name)]
                        (case (count matches)
                          0 (let [create-result (matrix/create-room! matrix-gateway {:name room-name
                                                                                     :invite invite
                                                                                     :encrypted true})]
                              (remember-slot-room-from-matrix-room!
                               state* project-id slot room-name create-result))
                          1 (remember-slot-room-from-matrix-room!
                             state* project-id slot room-name (first matches))
                          (throw (ex-info "Multiple joined Matrix rooms match the requested slot room name."
                                          {:code :slot_room_ambiguous
                                           :project-id project-id
                                           :slot slot
                                           :room-name room-name
                                           :room-ids (mapv :roomId matches)})))))]
    (when (seq invite)
      (matrix/ensure-users-power-level! matrix-gateway {:roomId (:roomId slot-room)
                                                        :users (vec invite)
                                                        :level 100}))
    slot-room))

(defn acquire-slot-handler
  [{:keys [state* matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (idempotent!
        env request
        #(let [project (:project body)
               project-id (:id project)
               leases (get-in @state* [:slots project-id] {})
               active-leases (into {} (filter (comp slots/active-lease? val) leases))
               slot (slots/first-free-slot active-leases)
               room-name (str project-id "-" slot)
               invite (:invite body)
               slot-room (ensure-slot-room! state* matrix-gateway project-id slot room-name invite)
               lease (state/acquire-slot! state* {:now (System/currentTimeMillis)}
                                          {:client-id (:clientId body)
                                           :project project
                                           :room-id (:roomId slot-room)
                                           :room-name (:name slot-room)})]
           {:slot (:slot lease)
            :roomId (:room-id lease)
            :roomName (:room-name lease)}))))))

(defn list-slots-handler
  [{:keys [state*]}]
  (fn [request]
    (json/ok-response (state/list-slots @state* (get-in request [:query-params "projectId" ])))))

(defn release-slot-handler
  [{:keys [state*]}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (state/release-slot!
        state*
        {:client-id (or (:client-id body) (:clientId body))
         :room-id (or (:room-id body) (:roomId body))
         :slot (:slot body)})))))

(defn verification-start-handler
  [{:keys [matrix-gateway] :as env}]
  (fn [request]
    (json/ok-response (idempotent! env request #(matrix/verification-start! matrix-gateway (body-params request))))))

(defn verification-confirm-handler
  [{:keys [matrix-gateway]}]
  (fn [request]
    (json/ok-response (matrix/verification-confirm! matrix-gateway (get-in request [:path-params :verificationId])))))

(defn verification-cancel-handler
  [{:keys [matrix-gateway]}]
  (fn [request]
    (json/ok-response (matrix/verification-cancel! matrix-gateway (get-in request [:path-params :verificationId])))))

(defn verification-status-handler
  [{:keys [matrix-gateway]}]
  (fn [_]
    (json/ok-response (matrix/verification-status matrix-gateway))))

(defn routes
  [env]
  [["/v1"
    ["/health" {:get (health-handler env)}]
    ["/clients" {:post (register-client-handler env)}]
    ["/clients/:clientId/subscriptions" {:patch (update-subscriptions-handler env)}]
    ["/clients/:clientId/heartbeat" {:post (heartbeat-handler env)}]
    ["/clients/:clientId" {:delete (unregister-client-handler env)}]
    ["/clients/:clientId/events" {:get (event-stream-handler env)}]
    ["/clients/:clientId/acks" {:post (acks-handler env)}]
    ["/slots" {:get (list-slots-handler env)}]
    ["/slots/acquire" {:post (acquire-slot-handler env)}]
    ["/slots/release" {:post (release-slot-handler env)}]
    ["/matrix/messages" {:post (send-message-handler env)}]
    ["/matrix/typing" {:post (typing-handler env)}]
    ["/matrix/reactions" {:post (send-reaction-handler env)}]
    ["/matrix/files" {:post (send-file-handler env)}]
    ["/matrix/media/download" {:post (download-media-handler env)}]
    ["/matrix/media/transcribe" {:post (transcribe-media-handler env)}]
    ["/matrix/rooms/resolve" {:post (resolve-room-handler env)}]
    ["/matrix/rooms" {:get (list-rooms-handler env)
                       :post (create-room-handler env)}]
    ["/verification/start" {:post (verification-start-handler env)}]
    ["/verification/:verificationId/confirm" {:post (verification-confirm-handler env)}]
    ["/verification/:verificationId/cancel" {:post (verification-cancel-handler env)}]
    ["/verification/status" {:get (verification-status-handler env)}]]])

(defn app
  [env]
  (wrap-errors
   (wrap-json-body
    (wrap-params
     (ring/ring-handler
      (ring/router (routes env))
      (ring/create-default-handler
       {:not-found (constantly (json/error-response 404 :not_found "Route not found" {}))
        :method-not-allowed (constantly (json/error-response 405 :method_not_allowed "Method not allowed" {}))
        :not-acceptable (constantly (json/error-response 406 :not_acceptable "Not acceptable" {}))}))))))
