(ns pi-matrix-relay.broker.matrix.trixnity
  (:require [clojure.string :as str]
            [missionary.core :as m]
            [ol.trixnity.client :as client]
            [ol.trixnity.event :as event]
            [ol.trixnity.repo :as repo]
            [ol.trixnity.room :as room]
            [ol.trixnity.room.message :as msg]
            [ol.trixnity.schemas :as mx]
            [pi-matrix-relay.broker.matrix :as matrix])
  (:import [java.time Duration Instant]))

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
  [{:keys [name roomName]}]
  (or name roomName))

(defn- now-iso []
  (str (Instant/now)))

(defn- attachment
  [ev]
  (when (or (event/url ev) (event/encrypted-file ev))
    {:attachmentId (event/event-id ev)
     :kind (case (event/msgtype ev)
             "m.image" "image"
             "m.audio" "audio"
             "m.video" "video"
             "m.file" "file"
             "other")
     :fileName (event/file-name ev)
     :mimeType (event/mime-type ev)
     :byteSize (event/size-bytes ev)
     :matrixContentUri (event/url ev)}))

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
               :room {:roomId (event/room-id ev)}
               :event {:eventId (event/event-id ev)
                       :sender sender
                       :senderDisplayName (event/sender-display-name ev)
                       :senderIsBot sender-is-bot?
                       :timestamp (now-iso)
                       :msgtype (event/msgtype ev)
                       :text (event/body ev)
                       :replyToEventId (event/relation-event-id ev)}
               :attachments (cond-> []
                              (attachment ev) (conj (attachment ev)))}}

       (event/reaction? ev)
       {:event "matrix.reaction"
        :data {:type "matrix.reaction"
               :room {:roomId (event/room-id ev)}
               :event {:eventId (event/event-id ev)
                       :sender sender
                       :senderIsBot sender-is-bot?
                       :timestamp (now-iso)
                       :reactsToEventId (event/relation-event-id ev)
                       :key (event/key ev)}}}))))

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
       :matrix {:connected true
                :userId (str (client/current-user-id c))
                :syncState (some-> (client/current-sync-state c) name)
                :encrypted (get-in config [:matrix :encrypted?] true)}}
      {:status "degraded"
       :matrix {:connected false
                :encrypted (get-in config [:matrix :encrypted?] true)}}))
  (resolve-room! [_ room-id-or-alias]
    (let [c (:client @runtime*)
          room-id (m/? (room/join-room c room-id-or-alias {::mx/timeout (Duration/ofSeconds 15)}))]
      {:roomId (str room-id)
       :canonicalAlias (when (str/starts-with? room-id-or-alias "#") room-id-or-alias)
       :name room-id-or-alias}))
  (create-room! [_ request]
    (let [c (:client @runtime*)
          invite (vec (:invite request))
          room-id (m/? (room/create-room c (cond-> {::mx/room-name (room-name-from-request request)
                                                    ::mx/visibility :private
                                                    ::mx/preset :private-chat}
                                             (seq invite) (assoc ::mx/invite invite))))]
      {:roomId (str room-id)
       :name (room-name-from-request request)}))
  (send-message! [_ {:keys [target body formattedBody replyTo]}]
    (let [c (:client @runtime*)
          room-id (:roomId target)
          message (msg/text body (cond-> {}
                                   formattedBody (assoc ::mx/format "org.matrix.custom.html"
                                                        ::mx/formatted-body formattedBody)))
          handle (m/? (room/send-message c room-id message {::mx/timeout (Duration/ofSeconds 10)}))
          tx (str (::mx/transaction-id handle))]
      {:roomId room-id
       :eventId tx
       :transactionId tx
       :replyTo replyTo}))
  (set-typing! [_ {:keys [roomId typing timeoutMs]}]
    (let [c (:client @runtime*)]
      (m/? (room/set-typing c roomId (boolean typing) {::mx/timeout (duration-ms (or timeoutMs 30000))}))
      {}))
  (send-file! [_ {:keys [target path name mimeType caption replyTo]}]
    (let [c (:client @runtime*)
          room-id (:roomId target)
          message (if (and mimeType (str/starts-with? mimeType "image/"))
                    (msg/image path (cond-> {}
                                      name (assoc ::mx/file-name name)
                                      mimeType (assoc ::mx/mime-type mimeType)
                                      caption (assoc ::mx/body caption)))
                    (msg/file path (cond-> {}
                                     name (assoc ::mx/file-name name)
                                     mimeType (assoc ::mx/mime-type mimeType)
                                     caption (assoc ::mx/body caption))))
          handle (m/? (room/send-message c room-id message {::mx/timeout (Duration/ofSeconds 30)}))
          tx (str (::mx/transaction-id handle))]
      {:roomId room-id
       :eventId tx
       :transactionId tx
       :replyTo replyTo}))
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
