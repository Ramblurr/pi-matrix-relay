(ns pi-matrix-relay.broker.matrix.trixnity
  (:require [charred.api :as charred]
            [clojure.string :as str]
            [missionary.core :as m]
            [org.httpkit.client :as http]
            [ol.trixnity.client :as client]
            [ol.trixnity.event :as event]
            [ol.trixnity.repo :as repo]
            [ol.trixnity.room :as room]
            [ol.trixnity.room.message :as msg]
            [ol.trixnity.schemas :as mx]
            [pi-matrix-relay.broker.matrix :as matrix])
  (:import [java.net URLEncoder]
           [java.time Duration Instant]))

(defn- require-nonblank
  [config k]
  (let [value (get config k)]
    (when (str/blank? (str value))
      (throw (matrix/ex :matrix_config_missing
                        (str "Missing Matrix config key " k)
                        {:key k})))
    value))

(defn- duration-ms
  [ms]
  (Duration/ofMillis (long ms)))

(defn- room-name-from-request
  [request]
  (:room/name request))

(defn- reply-target-event
  [target]
  {::mx/room-id (:room/id target)
   ::mx/event-id (:event/id target)})

(defn- text-message
  [request]
  (let [body (:body request)
        formatted-body (:formatted-body request)
        reply-to (:reply-to request)]
    (cond-> (msg/text body (cond-> {}
                             formatted-body (assoc ::mx/format "org.matrix.custom.html"
                                                   ::mx/formatted-body formatted-body)))
      reply-to (msg/reply-to (reply-target-event reply-to)))))

(defn- now-iso []
  (str (Instant/now)))

(defn- flow-value
  [flow]
  (m/? (->> flow
            (m/eduction (take 1))
            (m/reduce (fn [_ value] value) nil))))

(defn- normalized-room
  [room]
  {:room/id (::mx/room-id room)
   :room/name (::mx/room-name room)
   :room/membership (::mx/membership room)
   :room/direct? (::mx/is-direct room)})

(defn- read-matrix-json
  [body]
  (when-not (str/blank? (str body))
    (charred/read-json body)))

(defn- matrix-json-request!
  [{:keys [homeserver token method path body allow-not-found?]}]
  (let [response @(http/request (cond-> {:method method
                                         :url (str (str/replace homeserver #"/$" "") path)
                                         :headers (cond-> {"Accept" "application/json"}
                                                    token (assoc "Authorization" (str "Bearer " token))
                                                    body (assoc "Content-Type" "application/json"))
                                         :timeout 20000}
                                  body (assoc :body (charred/write-json-str body))))
        status (:status response)]
    (cond
      (and allow-not-found? (= 404 status))
      nil

      (>= status 400)
      (throw (matrix/ex :matrix_http_failed
                        "Matrix HTTP request failed."
                        {:status status
                         :path path
                         :body (:body response)}))

      :else
      (read-matrix-json (:body response)))))

(defn- matrix-login-token!
  [matrix-config]
  (let [password (:password matrix-config)]
    (when (str/blank? (str password))
      (throw (matrix/ex :matrix_config_missing
                        "Matrix password is required for broker-side room administration."
                        {})))
    (get (matrix-json-request!
          {:homeserver (:homeserver-url matrix-config)
           :method :post
           :path "/_matrix/client/v3/login"
           :body {"type" "m.login.password"
                  "identifier" {"type" "m.id.user"
                                "user" (:user-id matrix-config)}
                  "password" password}})
         "access_token")))

(defn- matrix-http-token!
  [config runtime*]
  (or (get-in config [:matrix :access-token])
      (:matrix-http-token @runtime*)
      (let [token (matrix-login-token! (:matrix config))]
        (swap! runtime* assoc :matrix-http-token token)
        token)))

(defn- encode-path-segment
  [value]
  (URLEncoder/encode (str value) "UTF-8"))

(defn- room-state-path
  [room-id event-type]
  (str "/_matrix/client/v3/rooms/" (encode-path-segment room-id)
       "/state/" event-type))

(defn- power-level-content!
  [config runtime* room-id]
  (or (matrix-json-request!
       {:homeserver (get-in config [:matrix :homeserver-url])
        :token (matrix-http-token! config runtime*)
        :method :get
        :path (room-state-path room-id "m.room.power_levels")
        :allow-not-found? true})
      {}))

(defn- put-power-level-content!
  [config runtime* room-id content]
  (matrix-json-request!
   {:homeserver (get-in config [:matrix :homeserver-url])
    :token (matrix-http-token! config runtime*)
    :method :put
    :path (room-state-path room-id "m.room.power_levels")
    :body content}))

(defn- attachment
  [ev]
  (when (or (event/url ev) (event/encrypted-file ev))
    {:attachment/id (event/event-id ev)
     :attachment/kind (case (event/msgtype ev)
                        "m.image" "image"
                        "m.audio" "audio"
                        "m.video" "video"
                        "m.file" "file"
                        "other")
     :file/name (event/file-name ev)
     :file/mime-type (event/mime-type ev)
     :file/byte-size (event/size-bytes ev)
     :matrix/content-uri (event/url ev)}))

(defn- normalized-event
  ([ev]
   (normalized-event ev nil))
  ([ev bot-user-id]
   (let [sender (event/sender ev)
         sender-is-bot? (= sender bot-user-id)]
     (cond
       (event/text? ev)
       {:event "matrix.message"
        :data {:type "matrix.message"
               :room/id (event/room-id ev)
               :event/id (event/event-id ev)
               :event/sender sender
               :event/sender-display-name (event/sender-display-name ev)
               :event/sender-is-bot? sender-is-bot?
               :event/timestamp (now-iso)
               :message/type (event/msgtype ev)
               :event/text (event/body ev)
               :event/reply-to-id (event/relation-event-id ev)
               :attachments (cond-> []
                              (attachment ev) (conj (attachment ev)))}}

       (event/reaction? ev)
       {:event "matrix.reaction"
        :data {:type "matrix.reaction"
               :room/id (event/room-id ev)
               :event/id (event/event-id ev)
               :event/sender sender
               :event/sender-is-bot? sender-is-bot?
               :event/timestamp (now-iso)
               :event/reacts-to-id (event/relation-event-id ev)
               :reaction/key (event/key ev)}}))))

(defn- start-virtual-thread!
  [f]
  (Thread/startVirtualThread
   (reify Runnable
     (run [_] (f)))))

(defn- start-timeline-loop!
  [client event-sink]
  (when-let [publish! (:publish! event-sink)]
    (let [bot-user-id (str (client/current-user-id client))]
      (start-virtual-thread!
       (fn []
         (m/?
          (m/reduce
           (fn [_ ev]
             (when-let [normalized (normalized-event ev bot-user-id)]
               (publish! normalized))
             nil)
           nil
           (room/get-timeline-events-from-now-on
            client
            {::mx/decryption-timeout (Duration/ofSeconds 8)}))))))))

(defrecord TrixnityGateway [config paths event-sink runtime*]
  matrix/MatrixGateway
  (start! [this]
    (when-not (:client @runtime*)
      (let [matrix (:matrix config)
            password (:password matrix)]
        (when (and (:access-token matrix) (str/blank? (str password)))
          (throw (matrix/ex :matrix_access_token_unsupported
                            "trixnity-clj currently requires password login; access-token login is not supported by this adapter yet."
                            {})))
        (let [client-config (merge
                             {::mx/homeserver-url (require-nonblank matrix :homeserver-url)
                              ::mx/user-id (require-nonblank matrix :user-id)
                              ::mx/password (require-nonblank matrix :password)
                              ::mx/device-name (or (:device-name matrix) "pi-matrix-relay-broker")}
                             (repo/sqlite4clj-config
                              {:database-path (:database-path paths)
                               :media-path (:media-dir paths)}))
              opened (m/? (client/open client-config))]
          (m/? (client/start-sync opened))
          (m/? (client/await-running opened {::mx/timeout (Duration/ofSeconds 30)}))
          (reset! runtime* {:client opened
                            :user-id (client/current-user-id opened)
                            :timeline-loop (start-timeline-loop! opened event-sink)
                            :started-at (System/currentTimeMillis)}))))
    this)
  (stop! [_]
    (when-let [c (:client @runtime*)]
      (try (m/? (client/stop-sync c)) (catch Throwable _ nil))
      (try (m/? (client/close c)) (catch Throwable _ nil))
      (reset! runtime* {}))
    nil)
  (health [_]
    (if-let [c (:client @runtime*)]
      {:status "ok"
       :matrix/connected? true
       :user/id (str (client/current-user-id c))
       :matrix/sync-state (some-> (client/current-sync-state c) name)
       :matrix/encrypted? (get-in config [:matrix :encrypted?] true)}
      {:status "degraded"
       :matrix/connected? false
       :matrix/encrypted? (get-in config [:matrix :encrypted?] true)}))
  (list-rooms! [_]
    (let [c (:client @runtime*)]
      (->> (or (flow-value (room/get-all-flat c)) [])
           (mapv normalized-room))))
  (resolve-room! [_ room-id-or-alias]
    (let [c (:client @runtime*)
          room-id (m/? (room/join-room c room-id-or-alias {::mx/timeout (Duration/ofSeconds 15)}))]
      {:room/id (str room-id)
       :room/canonical-alias (when (str/starts-with? room-id-or-alias "#") room-id-or-alias)
       :room/name room-id-or-alias}))
  (create-room! [_ request]
    (let [c (:client @runtime*)
          invite (vec (:invite request))
          room-id (m/? (room/create-room c (cond-> {::mx/room-name (room-name-from-request request)
                                                    ::mx/visibility :private
                                                    ::mx/preset :private-chat}
                                             (seq invite) (assoc ::mx/invite invite))))]
      {:room/id (str room-id)
       :room/name (room-name-from-request request)}))
  (ensure-users-power-level! [_ request]
    (let [room-id (:room/id request)
          level (long (or (:level request) 100))
          users (vec (:users request))
          content (power-level-content! config runtime* room-id)
          current-users (or (get content "users") {})
          updated-content (assoc content "users" (reduce #(assoc %1 %2 level) current-users users))]
      (when (not= content updated-content)
        (put-power-level-content! config runtime* room-id updated-content))
      {:room/id room-id
       :users users
       :level level}))
  (leave-room! [_ request]
    (let [c (:client @runtime*)
          room-id (:room/id request)]
      (m/? (room/leave-room c room-id (cond-> {}
                                        (:reason request) (assoc ::mx/reason (:reason request)))))
      {:room/id room-id
       :left true}))
  (send-message! [_ {:keys [target] :as request}]
    (let [c (:client @runtime*)
          room-id (:room/id target)
          message (text-message request)
          handle (m/? (room/send-message c room-id message {::mx/timeout (Duration/ofSeconds 10)}))
          tx (str (::mx/transaction-id handle))]
      {:room/id room-id
       :event/id tx
       :transaction/id tx
       :reply-to (:reply-to request)}))
  (set-typing! [_ request]
    (let [c (:client @runtime*)
          room-id (:room/id request)]
      (m/? (room/set-typing c room-id (boolean (:typing request)) {::mx/timeout (duration-ms (or (:timeout/ms request) 30000))}))
      {}))
  (send-reaction! [_ request]
    (let [c (:client @runtime*)
          room-id (:room/id request)
          event-id (:event/id request)
          key (:key request)
          tx (m/? (room/send-reaction c
                                      room-id
                                      (reply-target-event {:room/id room-id :event/id event-id})
                                      key
                                      {::mx/timeout (Duration/ofSeconds 10)}))]
      {:room/id room-id
       :event/id (str tx)
       :transaction/id (str tx)
       :event/reacts-to-id event-id
       :key key}))
  (send-file! [_ {:keys [target path name caption] :as request}]
    (let [c (:client @runtime*)
          room-id (:room/id target)
          mime-type (:file/mime-type request)
          message (if (and mime-type (str/starts-with? mime-type "image/"))
                    (msg/image path (cond-> {}
                                      name (assoc ::mx/file-name name)
                                      mime-type (assoc ::mx/mime-type mime-type)
                                      caption (assoc ::mx/body caption)))
                    (msg/file path (cond-> {}
                                     name (assoc ::mx/file-name name)
                                     mime-type (assoc ::mx/mime-type mime-type)
                                     caption (assoc ::mx/body caption))))
          handle (m/? (room/send-message c room-id message {::mx/timeout (Duration/ofSeconds 30)}))
          tx (str (::mx/transaction-id handle))]
      {:room/id room-id
       :event/id tx
       :transaction/id tx
       :reply-to (:reply-to request)}))
  (download-media! [_ request]
    (matrix/unavailable :media_download_unavailable "Matrix media download needs event attachment lookup before implementation." {:request request}))
  (transcribe-media! [_ request]
    (matrix/unavailable :transcription_unavailable "Broker-side transcription is not available." {:request request}))
  (verification-start! [_ request]
    (matrix/unavailable :verification_unavailable "Matrix verification is not implemented yet." {:request request}))
  (verification-confirm! [_ verification-id]
    (matrix/unavailable :verification_unavailable "Matrix verification is not implemented yet." {:verification-id verification-id}))
  (verification-cancel! [_ verification-id]
    (matrix/unavailable :verification_unavailable "Matrix verification is not implemented yet." {:verification-id verification-id}))
  (verification-status [_]
    {:verifications []}))

(defn gateway
  ([config paths]
   (gateway config paths nil))
  ([config paths event-sink]
   (->TrixnityGateway config paths event-sink (atom {}))))
