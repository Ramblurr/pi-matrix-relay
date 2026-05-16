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
                    (events/unsubscribe! subscribers* client-id channel))}))))

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

(defn ensure-send-authorized!
  [state client-id room-id]
  (when-not (state/known-room-for-client? state client-id room-id)
    (throw (ex-info "Client is not registered for the target Matrix room."
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
               create-result (matrix/create-room! matrix-gateway {:name room-name
                                                                  :invite (:invite body)
                                                                  :encrypted true})
               lease (state/acquire-slot! state* {:now (System/currentTimeMillis)}
                                          {:client-id (:clientId body)
                                           :project project
                                           :room-id (:roomId create-result)
                                           :room-name (:name create-result)})]
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
    (json/ok-response (state/release-slot! state* (body-params request)))))

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
    ["/matrix/files" {:post (send-file-handler env)}]
    ["/matrix/media/download" {:post (download-media-handler env)}]
    ["/matrix/media/transcribe" {:post (transcribe-media-handler env)}]
    ["/matrix/rooms/resolve" {:post (resolve-room-handler env)}]
    ["/matrix/rooms" {:post (create-room-handler env)}]
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
