(ns pi-matrix-relay.broker.api
  (:require [clojure.string :as str]
            [org.httpkit.server :as hk]
            [reitit.ring :as ring]
            [ring.middleware.params :refer [wrap-params]]
            [pi-matrix-relay.broker.api.presenters :as present]
            [pi-matrix-relay.broker.db :as broker-db]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.runtime :as runtime]
            [pi-matrix-relay.broker.store :as store]))

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
                 :idempotency_conflict 409
                 :request_in_progress 409
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

(def retry-after-ms
  {:default 1000
   :slot/acquire 5000})

(defn- idempotent!
  [{:keys [db-conn]} operation request thunk]
  (if-let [rid (request-id request)]
    (let [payload (dissoc (body-params request) :requestId)
          owner-id (random-uuid)
          reservation (store/reserve-request!
                       db-conn
                       {:request-id rid
                        :operation operation
                        :fingerprint (store/request-fingerprint operation payload)
                        :owner-id owner-id
                        :now-ms (System/currentTimeMillis)
                        :retry-after-ms (get retry-after-ms operation (:default retry-after-ms))})]
      (case (:status reservation)
        :reserved (let [result (thunk)]
                    (store/complete-request! db-conn {:request-id rid
                                                      :now-ms (System/currentTimeMillis)}
                                             result)
                    result)
        :completed (:result reservation)
        :pending (throw (ex-info "Request is still in progress. Retry later."
                                 {:code :request_in_progress
                                  :request-id rid
                                  :retryAfterMs (:retry-after-ms reservation)}))
        reservation))
    (thunk)))

(defn health-handler
  [{:keys [matrix-gateway]}]
  (fn [_]
    (json/ok-response (matrix/health matrix-gateway))))

(defn register-client-handler
  [{:keys [db-conn config] :as env}]
  (fn [request]
    (json/ok-response
     (idempotent!
      env :client/register request
      #(present/client-registration
        (store/register-client!
         db-conn
         {:heartbeat-seconds (get-in config [:leases :heartbeat-seconds] 30)
          :global-operators (get-in config [:matrix :operators] [])}
         (body-params request)))))))

(defn update-subscriptions-handler
  [{:keys [db-conn]}]
  (fn [request]
    (let [client-id (client-id-param request)
          body (body-params request)]
      (json/ok-response (present/subscriptions
                         (store/update-subscriptions! db-conn client-id (:rooms body)))))))

(defn- room-id-param
  [request]
  (get-in request [:path-params :roomId]))

(defn- ensure-room-delivery-mode-authorized!
  [db client-id room-id]
  (when-not (store/known-room-for-client? db client-id room-id)
    (throw (ex-info "Client is not registered for the target Matrix room."
                    {:code :room_not_allowed
                     :client-id client-id
                     :room-id room-id}))))

(defn get-room-delivery-mode-handler
  [{:keys [db-conn]}]
  (fn [request]
    (let [client-id (client-id-param request)
          room-id (room-id-param request)]
      (ensure-room-delivery-mode-authorized! @db-conn client-id room-id)
      (json/ok-response (present/room-delivery-mode
                         (store/room-delivery-mode @db-conn room-id))))))

(defn set-room-delivery-mode-handler
  [{:keys [db-conn]}]
  (fn [request]
    (let [client-id (client-id-param request)
          room-id (room-id-param request)
          body (body-params request)]
      (json/ok-response (present/room-delivery-mode
                         (store/set-room-default-delivery-mode!
                          db-conn
                          {:client-id client-id
                           :room-id room-id
                           :default-delivery-mode (:defaultDeliveryMode body)
                           :updated-by-user (:updatedByUser body)
                           :now-ms (System/currentTimeMillis)}))))))

(defn heartbeat-handler
  [{:keys [db-conn config]}]
  (fn [request]
    (let [client-id (client-id-param request)
          now (System/currentTimeMillis)]
      (store/heartbeat! db-conn client-id now)
      (json/ok-response (present/heartbeat (get-in config [:leases :heartbeat-seconds] 30))))))

(defn unregister-client-handler
  [{:keys [db-conn]}]
  (fn [request]
    (json/ok-response (store/unregister-client! db-conn (client-id-param request) (System/currentTimeMillis)))))

(defn acks-handler
  [_]
  (fn [request]
    (json/ok-response {:accepted (count (:acks (body-params request)))})))

(defn- replay-allowed?
  [db client-id event]
  (if-let [room-id (get-in event [:data :room :roomId])]
    (store/known-room-for-client? db client-id room-id)
    true))

(defn event-stream-handler
  [{:keys [db-conn runtime]}]
  (fn [request]
    (let [client-id (client-id-param request)
          last-event-id (get-in request [:headers "last-event-id"])]
      (store/require-client @db-conn client-id)
      (hk/as-channel
       request
       {:on-open (fn [channel]
                   (hk/send! channel {:status 200
                                      :headers {"Content-Type" "text/event-stream"
                                                "Cache-Control" "no-cache"
                                                "Connection" "keep-alive"}
                                      :body ": connected\n\n"}
                             false)
                   (events/subscribe! runtime client-id channel)
                   (doseq [event (filter #(replay-allowed? @db-conn client-id %)
                                         (runtime/replay-events-after runtime last-event-id))]
                     (hk/send! channel (events/format-sse event) false)))
        :on-close (fn [channel _]
                    (events/unsubscribe! runtime client-id channel)
                    (store/mark-client-suspect! db-conn client-id (System/currentTimeMillis)))}))))

(defn resolve-room-handler
  [{:keys [matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (idempotent! env :matrix/resolve-room request #(matrix/resolve-room! matrix-gateway (:room body)))))))

(defn create-room-handler
  [{:keys [matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (idempotent! env :matrix/create-room request #(matrix/create-room! matrix-gateway body))))))

(defn- joined-room?
  [room]
  (= "join" (:membership room)))

(defn list-rooms-handler
  [{:keys [matrix-gateway]}]
  (fn [_]
    (json/ok-response {:rooms (->> (matrix/list-rooms! matrix-gateway)
                                   (filter joined-room?)
                                   vec)})))

(defn- joined-room-id?
  [matrix-gateway room-id]
  (boolean (some #(and (= room-id (:roomId %))
                       (joined-room? %))
                 (matrix/list-rooms! matrix-gateway))))

(defn ensure-send-authorized!
  [db matrix-gateway client-id room-id]
  (when-not (if client-id
              (store/known-room-for-client? db client-id room-id)
              (joined-room-id? matrix-gateway room-id))
    (throw (ex-info (if client-id
                      "Client is not registered for the target Matrix room."
                      "Target Matrix room is not currently joined.")
                    {:code :room_not_allowed
                     :client-id client-id
                     :room-id room-id}))))

(defn send-message-handler
  [{:keys [db-conn matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (get-in body [:target :roomId])]
      (ensure-send-authorized! @db-conn matrix-gateway client-id room-id)
      (json/ok-response
       (idempotent! env :matrix/send-message request #(matrix/send-message! matrix-gateway body))))))

(defn typing-handler
  [{:keys [db-conn matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (:roomId body)]
      (ensure-send-authorized! @db-conn matrix-gateway client-id room-id)
      (json/ok-response
       (idempotent! env :matrix/typing request #(matrix/set-typing! matrix-gateway body))))))

(defn send-reaction-handler
  [{:keys [db-conn matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (:roomId body)]
      (ensure-send-authorized! @db-conn matrix-gateway client-id room-id)
      (json/ok-response
       (idempotent! env :matrix/send-reaction request #(matrix/send-reaction! matrix-gateway body))))))

(defn send-file-handler
  [{:keys [db-conn matrix-gateway] :as env}]
  (fn [request]
    (let [body (body-params request)
          client-id (:clientId body)
          room-id (get-in body [:target :roomId])]
      (ensure-send-authorized! @db-conn matrix-gateway client-id room-id)
      (json/ok-response
       (idempotent! env :matrix/send-file request #(matrix/send-file! matrix-gateway body))))))

(defn download-media-handler
  [{:keys [matrix-gateway]}]
  (fn [request]
    (json/ok-response (matrix/download-media! matrix-gateway (body-params request)))))

(defn transcribe-media-handler
  [{:keys [matrix-gateway]}]
  (fn [request]
    (json/ok-response (matrix/transcribe-media! matrix-gateway (body-params request)))))

(defn- room-display-name
  [room]
  (or (:name room) (:roomName room)))

(defn- matching-joined-slot-rooms
  [matrix-gateway room-name]
  (->> (matrix/list-rooms! matrix-gateway)
       (filter joined-room?)
       (filter #(= room-name (room-display-name %)))
       (sort-by :roomId)
       vec))

(defn- remember-slot-room-from-matrix-room!
  [db-conn project slot room-name room]
  (store/remember-slot-room!
   db-conn
   {:project project
    :slot slot
    :room-id (:roomId room)
    :room-name (or (room-display-name room) room-name)}))

(defn- ensure-slot-room!
  [{:keys [db-conn matrix-gateway]} project slot room-name invite]
  (let [project-id (:id project)
        slot-room (or (store/slot-room @db-conn project-id slot)
                      (let [matches (matching-joined-slot-rooms matrix-gateway room-name)]
                        (case (count matches)
                          0 (let [create-result (matrix/create-room! matrix-gateway {:name room-name
                                                                                     :invite invite
                                                                                     :encrypted true})]
                              (remember-slot-room-from-matrix-room!
                               db-conn project slot room-name create-result))
                          1 (remember-slot-room-from-matrix-room!
                             db-conn project slot room-name (first matches))
                          (throw (ex-info "Multiple joined Matrix rooms match the requested slot room name."
                                          {:code :slot_room_ambiguous
                                           :project-id project-id
                                           :slot slot
                                           :room-name room-name
                                           :room-ids (mapv :roomId matches)})))))]
    (when (seq invite)
      (matrix/ensure-users-power-level! matrix-gateway {:roomId (:room-id slot-room)
                                                        :users (vec invite)
                                                        :level 100}))
    slot-room))

(defn acquire-slot-handler
  [{:keys [db-conn] :as env}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (idempotent!
        env :slot/acquire request
        #(let [project (:project body)
               project-id (:id project)
               invite (:invite body)
               reservation (store/reserve-slot! db-conn {:now-ms (System/currentTimeMillis)}
                                                {:client-id (:clientId body)
                                                 :project project})]
           (try
             (let [slot (:slot reservation)
                   room-name (str project-id "-" slot)
                   slot-room (ensure-slot-room! env project slot room-name invite)
                   lease (store/complete-slot-reservation!
                          db-conn
                          {:now-ms (System/currentTimeMillis)
                           :lease-id (:lease-id reservation)
                           :reservation-id (:reservation-id reservation)
                           :client-id (:clientId body)
                           :room-id (:room-id slot-room)
                           :room-name (:room-name slot-room)})]
               (present/slot-acquire lease))
             (catch Throwable t
               (store/abandon-slot-reservation! db-conn (assoc reservation
                                                               :now-ms (System/currentTimeMillis)))
               (throw t)))))))))

(defn list-slots-handler
  [{:keys [db-conn]}]
  (fn [request]
    (json/ok-response (present/slots-list
                       (store/list-slots @db-conn (get-in request [:query-params "projectId"]))))))

(defn release-slot-handler
  [{:keys [db-conn]}]
  (fn [request]
    (let [body (body-params request)]
      (json/ok-response
       (store/release-slot!
        db-conn
        {:now-ms (System/currentTimeMillis)
         :client-id (or (:client-id body) (:clientId body))
         :room-id (or (:room-id body) (:roomId body))
         :slot (:slot body)})))))

(defn verification-start-handler
  [{:keys [matrix-gateway] :as env}]
  (fn [request]
    (json/ok-response (idempotent! env :verification/start request #(matrix/verification-start! matrix-gateway (body-params request))))))

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

(defn- strip-suffix
  [s suffix]
  (when (and s (str/ends-with? s suffix))
    (subs s 0 (- (count s) (count suffix)))))

(defn- with-client-id
  [request client-id]
  (assoc-in request [:path-params :clientId] client-id))

(defn legacy-client-path-handler
  "Route client endpoints for legacy extensions that embedded slash-containing
  client IDs directly into the URL path instead of percent-encoding them."
  [env]
  (fn [request]
    (let [client-path (get-in request [:path-params :clientPath])
          method (:request-method request)]
      (cond
        (and (= :get method) (strip-suffix client-path "/events"))
        ((event-stream-handler env)
         (with-client-id request (strip-suffix client-path "/events")))

        (and (= :post method) (strip-suffix client-path "/heartbeat"))
        ((heartbeat-handler env)
         (with-client-id request (strip-suffix client-path "/heartbeat")))

        (and (= :patch method) (strip-suffix client-path "/subscriptions"))
        ((update-subscriptions-handler env)
         (with-client-id request (strip-suffix client-path "/subscriptions")))

        (and (= :post method) (strip-suffix client-path "/acks"))
        ((acks-handler env)
         (with-client-id request (strip-suffix client-path "/acks")))

        (= :delete method)
        ((unregister-client-handler env)
         (with-client-id request client-path))

        :else
        (json/error-response 404 :not_found "Route not found" {})))))

(defn- slashful-client-id?
  [client-id]
  (boolean (and client-id (str/includes? client-id "/"))))

(defn- legacy-client-path?
  [method client-path]
  (case method
    :get (slashful-client-id? (strip-suffix client-path "/events"))
    :post (or (slashful-client-id? (strip-suffix client-path "/heartbeat"))
              (slashful-client-id? (strip-suffix client-path "/acks")))
    :patch (slashful-client-id? (strip-suffix client-path "/subscriptions"))
    :delete (slashful-client-id? client-path)
    false))

(defn wrap-legacy-client-paths
  [handler env]
  (let [legacy-handler (legacy-client-path-handler env)
        prefix "/v1/clients/"]
    (fn [request]
      (let [uri (:uri request)
            client-path (when (and uri (str/starts-with? uri prefix))
                          (subs uri (count prefix)))]
        (if (legacy-client-path? (:request-method request) client-path)
          (legacy-handler (assoc-in request [:path-params :clientPath] client-path))
          (handler request))))))

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
    ["/clients/:clientId/rooms/:roomId/delivery-mode" {:get (get-room-delivery-mode-handler env)
                                                       :put (set-room-delivery-mode-handler env)}]
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
  (let [handler (ring/ring-handler
                 (ring/router (routes env))
                 (ring/create-default-handler
                  {:not-found (constantly (json/error-response 404 :not_found "Route not found" {}))
                   :method-not-allowed (constantly (json/error-response 405 :method_not_allowed "Method not allowed" {}))
                   :not-acceptable (constantly (json/error-response 406 :not_acceptable "Not acceptable" {}))}))]
    (wrap-errors
     (wrap-json-body
      (wrap-params
       (broker-db/wrap-db
        (wrap-legacy-client-paths handler env)
        (:db-conn env)))))))
