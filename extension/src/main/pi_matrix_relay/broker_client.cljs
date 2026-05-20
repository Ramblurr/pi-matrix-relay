(ns pi-matrix-relay.broker-client
  (:require [pi-matrix-relay.http :as http]))

(defn- client-path
  ([client-id]
   (client-path client-id nil))
  ([client-id suffix]
   (str "/v1/clients/" (http/encode-path-segment client-id) suffix)))

(defn- client-room-path
  [client-id room-id suffix]
  (str (client-path client-id "/rooms/")
       (http/encode-path-segment room-id)
       suffix))

(defn health!
  ([]
   (health! {}))
  ([opts]
   (http/request-edn! opts "GET" "/v1/health" nil)))

(defn register-client!
  ([request]
   (register-client! {} request))
  ([opts request]
   (http/request-edn! opts "POST" "/v1/clients" request)))

(defn update-subscriptions!
  ([client-id rooms]
   (update-subscriptions! {} client-id rooms))
  ([opts client-id rooms]
   (http/request-edn! opts "PATCH" (client-path client-id "/subscriptions")
                     {:rooms rooms})))

(defn get-room-delivery-mode!
  ([client-id room-id]
   (get-room-delivery-mode! {} client-id room-id))
  ([opts client-id room-id]
   (http/request-edn! opts "GET" (client-room-path client-id room-id "/delivery-mode") nil)))

(defn set-room-delivery-mode!
  ([client-id room-id default-delivery-mode updated-by-user]
   (set-room-delivery-mode! {} client-id room-id default-delivery-mode updated-by-user))
  ([opts client-id room-id default-delivery-mode updated-by-user]
   (http/request-edn! opts "PUT" (client-room-path client-id room-id "/delivery-mode")
                     {:room/default-delivery-mode default-delivery-mode
                      :room/default-delivery-mode-updated-by-user updated-by-user})))

(defn get-room-prompt-mode!
  ([client-id room-id]
   (get-room-prompt-mode! {} client-id room-id))
  ([opts client-id room-id]
   (http/request-edn! opts "GET" (client-room-path client-id room-id "/prompt-mode") nil)))

(defn set-room-prompt-mode!
  ([client-id room-id mode updated-by-user]
   (set-room-prompt-mode! {} client-id room-id mode updated-by-user))
  ([opts client-id room-id mode updated-by-user]
   (http/request-edn! opts "PUT" (client-room-path client-id room-id "/prompt-mode")
                     {:room/prompt-mode mode
                      :room/prompt-mode-updated-by-user updated-by-user})))

(defn get-room-tool-message-settings!
  ([client-id room-id]
   (get-room-tool-message-settings! {} client-id room-id))
  ([opts client-id room-id]
   (http/request-edn! opts "GET" (client-room-path client-id room-id "/tool-messages") nil)))

(defn set-room-tool-message-settings!
  ([client-id room-id settings updated-by-user]
   (set-room-tool-message-settings! {} client-id room-id settings updated-by-user))
  ([opts client-id room-id {:keys [enabled? batch-ms]} updated-by-user]
   (http/request-edn! opts "PUT" (client-room-path client-id room-id "/tool-messages")
                     (cond-> {}
                       (some? enabled?)
                       (assoc :room/tool-messages-enabled? enabled?)

                       (some? batch-ms)
                       (assoc :room/tool-message-batch-ms batch-ms)

                       (some? updated-by-user)
                       (assoc :room/tool-message-settings-updated-by-user updated-by-user)))))

(defn heartbeat!
  ([client-id]
   (heartbeat! {} client-id))
  ([opts client-id]
   (http/request-edn! opts "POST" (client-path client-id "/heartbeat") {})))

(defn unregister-client!
  ([client-id reason]
   (unregister-client! {} client-id reason))
  ([opts client-id reason]
   (http/request-edn! opts "DELETE" (client-path client-id) {:reason reason})))

(defn acquire-slot!
  ([client-id project invite]
   (acquire-slot! {} client-id project invite))
  ([opts client-id project invite]
   (http/request-edn! opts "POST" "/v1/slots/acquire"
                     (cond-> {:client/id client-id
                              :project project}
                       (seq invite) (assoc :invite invite)))))

(defn release-slot!
  ([client-id room-id slot]
   (release-slot! {} client-id room-id slot))
  ([opts client-id room-id slot]
   (http/request-edn! opts "POST" "/v1/slots/release"
                     {:client/id client-id
                      :room/id room-id
                      :slot slot})))

(defn list-slots!
  ([project-id]
   (list-slots! {} project-id))
  ([opts project-id]
   (http/request-edn! opts "GET" (str "/v1/slots?project-id=" (http/encode-path-segment project-id)) nil)))

(defn list-rooms!
  ([]
   (list-rooms! {}))
  ([opts]
   (http/request-edn! opts "GET" "/v1/matrix/rooms" nil)))

(defn open-event-stream!
  ([client-id on-event]
   (open-event-stream! {:env (.-env js/process)} client-id on-event))
  ([opts client-id on-event]
   (http/open-event-stream! (update opts :diagnostics merge {:client/id client-id})
                            (client-path client-id "/events")
                            on-event)))

(defn resolve-room!
  ([room]
   (resolve-room! {} room))
  ([opts room]
   (http/request-edn! opts "POST" "/v1/matrix/rooms/resolve" {:room room})))

(defn send-message!
  ([room-id message]
   (send-message! {} room-id message nil))
  ([opts-or-room-id room-id-or-message message-or-opts]
   (if (map? opts-or-room-id)
     (send-message! opts-or-room-id room-id-or-message message-or-opts nil)
     (send-message! {} opts-or-room-id room-id-or-message message-or-opts)))
  ([opts room-id message send-opts]
   (http/request-edn! opts "POST" "/v1/matrix/messages"
                     (cond-> {:target {:room/id room-id}
                              :body message}
                       (:client/id send-opts)
                       (assoc :client/id (:client/id send-opts))

                       (:reply-to/event-id send-opts)
                       (assoc :reply-to {:room/id room-id
                                         :event/id (:reply-to/event-id send-opts)})

                       (:formatted-body send-opts)
                       (assoc :formatted-body (:formatted-body send-opts))))))

(defn set-typing!
  ([room-id typing?]
   (set-typing! {} room-id typing? nil))
  ([opts-or-room-id room-id-or-typing? typing?-or-opts]
   (if (map? opts-or-room-id)
     (set-typing! opts-or-room-id room-id-or-typing? typing?-or-opts nil)
     (set-typing! {} opts-or-room-id room-id-or-typing? typing?-or-opts)))
  ([opts room-id typing? typing-opts]
   (http/request-edn! opts "POST" "/v1/matrix/typing"
                     (cond-> {:room/id room-id
                              :typing (boolean typing?)}
                       (:client/id typing-opts)
                       (assoc :client/id (:client/id typing-opts))

                       (:timeout/ms typing-opts)
                       (assoc :timeout/ms (:timeout/ms typing-opts))))))

(defn send-reaction!
  ([room-id event-id key]
   (send-reaction! {} room-id event-id key nil))
  ([opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts]
   (if (map? opts-or-room-id)
     (send-reaction! opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts nil)
     (send-reaction! {} opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts)))
  ([opts room-id event-id key send-opts]
   (http/request-edn! opts "POST" "/v1/matrix/reactions"
                     (cond-> {:room/id room-id
                              :event/id event-id
                              :key key}
                       (:client/id send-opts)
                       (assoc :client/id (:client/id send-opts))))))

(defn verification-start!
  ([request]
   (verification-start! {} request))
  ([opts request]
   (http/request-edn! opts "POST" "/v1/verification/start" request)))

(defn verification-bootstrap!
  ([]
   (verification-bootstrap! {} {}))
  ([request]
   (verification-bootstrap! {} request))
  ([opts request]
   (http/request-edn! opts "POST" "/v1/verification/bootstrap" request)))

(defn- verification-path
  [verification-id suffix]
  (str "/v1/verification/" (http/encode-path-segment verification-id) suffix))

(defn verification-accept!
  ([verification-id]
   (verification-accept! {} verification-id))
  ([opts verification-id]
   (http/request-edn! opts "POST" (verification-path verification-id "/accept") {})))

(defn verification-start-sas!
  ([verification-id]
   (verification-start-sas! {} verification-id))
  ([opts verification-id]
   (http/request-edn! opts "POST" (verification-path verification-id "/start-sas") {})))

(defn verification-confirm!
  ([verification-id]
   (verification-confirm! {} verification-id))
  ([opts verification-id]
   (http/request-edn! opts "POST" (verification-path verification-id "/confirm") {})))

(defn verification-no-match!
  ([verification-id]
   (verification-no-match! {} verification-id))
  ([opts verification-id]
   (http/request-edn! opts "POST" (verification-path verification-id "/no-match") {})))

(defn verification-cancel!
  ([verification-id]
   (verification-cancel! {} verification-id))
  ([opts verification-id]
   (http/request-edn! opts "POST" (verification-path verification-id "/cancel") {})))

(defn verification-status!
  ([]
   (verification-status! {}))
  ([opts]
   (http/request-edn! opts "GET" "/v1/verification/status" nil)))

(defn verification-targets!
  ([]
   (verification-targets! {}))
  ([opts]
   (http/request-edn! opts "GET" "/v1/verification/targets" nil)))
