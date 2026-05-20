(ns pi-matrix-relay.broker.matrix.trixnity
  (:require [clojure.string :as str]
            [missionary.core :as m]
            [ol.trixnity.client :as client]
            [ol.trixnity.event :as event]
            [ol.trixnity.key :as key]
            [ol.trixnity.repo :as repo]
            [ol.trixnity.room :as room]
            [ol.trixnity.room.message :as msg]
            [ol.trixnity.space :as space]
            [ol.trixnity.schemas :as mx]
            [ol.trixnity.verification :as verification]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.store :as store])
  (:import [de.connect2x.trixnity.core.model.events.m.room MemberEventContent]
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

(defn- persistent-map
  [value]
  (into {} (or value {})))

(defn- power-level-content-for-set
  [content]
  (let [content (dissoc (persistent-map content) ::mx/raw)]
    (cond-> content
      (::mx/event-levels content)
      (update ::mx/event-levels persistent-map)

      (::mx/user-levels content)
      (update ::mx/user-levels persistent-map)

      (::mx/notification-levels content)
      (update ::mx/notification-levels persistent-map))))

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
       (and (event/text? ev) (some? (event/body ev)))
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

(defn- unqualify-key
  [k]
  (if (keyword? k)
    (keyword (name k))
    k))

(defn- raw-key?
  [k]
  (and (keyword? k)
       (= "raw" (name k))))

(defn- wire-data
  [value]
  (cond
    (or (map? value) (instance? java.util.Map value))
    (into {}
          (keep (fn [[k v]]
                  (when-not (raw-key? k)
                    [(unqualify-key k) (wire-data v)])))
          value)

    (or (set? value) (instance? java.util.Set value))
    (set (map wire-data value))

    (or (sequential? value)
        (and (instance? java.lang.Iterable value)
             (not (string? value))))
    (mapv wire-data value)

    :else value))

(defn- verification-snapshots
  [c]
  (mapv wire-data (verification/status c)))

(defn- verification-task-result
  [task]
  (wire-data (m/? task)))

(defn- joined-direct-room?
  [room-snapshot]
  (and (= "join" (some-> (::mx/membership room-snapshot) name))
       (true? (::mx/is-direct room-snapshot))))

(defn- member-content-value
  [content k]
  (cond
    (map? content)
    (case k
      :membership (or (get content :membership) (::mx/membership content))
      :display-name (or (get content :display-name) (::mx/display-name content))
      (get content k))
    (instance? MemberEventContent content)
    (case k
      :membership (some-> ^MemberEventContent content .getMembership .getValue)
      :display-name (.getDisplayName ^MemberEventContent content)
      nil)
    :else nil))

(defn- joined-member?
  [content]
  (= "join" (some-> (member-content-value content :membership) name)))

(defn- member-display-name
  [content]
  (member-content-value content :display-name))

(defn- joined-dm-verification-targets
  [client bot-user-id]
  (let [rooms (or (flow-value (room/get-all-flat client)) [])]
    (->> rooms
         (filter joined-direct-room?)
         (mapcat (fn [room-snapshot]
                   (let [room-id (::mx/room-id room-snapshot)
                         member-flows (or (flow-value
                                           (room/get-all-state client room-id MemberEventContent))
                                          {})]
                     (keep (fn [[user-id event-flow]]
                             (let [state-event (flow-value event-flow)
                                   content (::mx/content state-event)]
                               (when (and (not= user-id bot-user-id)
                                          (joined-member? content))
                                 (cond-> {:user/id user-id
                                          :room/id room-id
                                          :room/direct? true}
                                   (::mx/room-name room-snapshot)
                                   (assoc :room/name (::mx/room-name room-snapshot))

                                   (member-display-name content)
                                   (assoc :user/display-name (member-display-name content))))))
                           member-flows))))
         (sort-by (juxt :user/id :room/id))
         vec)))

(defn- activate-room-verification-request!
  [client ev]
  (when (and (event/text? ev)
             (nil? (event/body ev)))
    (try
      (verification-task-result
       (verification/get-active-user-verification! client
                                                  (event/room-id ev)
                                                  (event/event-id ev)))
      (catch Throwable _
        nil))))

(defn- timeline-event-value
  [value]
  (try
    (if (map? value)
      value
      (flow-value value))
    (catch Throwable _
      value)))

(defn- activate-recent-room-verification-requests!
  [client room-id]
  (let [events (or (flow-value
                    (room/get-last-timeline-events-list
                     client
                     room-id
                     20
                     1
                     {::mx/decryption-timeout (Duration/ofSeconds 8)
                      ::mx/fetch-timeout (Duration/ofSeconds 8)}))
                   [])]
    (->> events
         (keep timeline-event-value)
         (keep #(activate-room-verification-request! client %))
         vec)))

(defn- kind-name
  [value]
  (cond
    (keyword? value) (name value)
    (string? value) value
    (some? value) (str value)))

(defn- verification-event-type
  [snapshot]
  (let [state-kind (kind-name (get-in snapshot [:verification-state :kind]))
        sas-kind (kind-name (get-in snapshot [:verification-state :sas-state :kind]))
        sas-emojis (seq (get-in snapshot [:verification-state :sas-state :sas-emojis]))]
    (cond
      sas-emojis "verification.emoji"
      (= "done" state-kind) "verification.done"
      (= "cancel" state-kind) "verification.cancelled"
      (or (= "their-request" state-kind)
          (= "own-request" state-kind)
          (= "ready" state-kind)
          (= "start" state-kind)
          (= "their-sas-start" sas-kind))
      "verification.requested")))

(defn- verification-event-data
  [event-type snapshot]
  (cond-> (assoc snapshot :type event-type)
    (= "verification.emoji" event-type)
    (assoc :emojis (get-in snapshot [:verification-state :sas-state :sas-emojis]))

    (= "verification.cancelled" event-type)
    (assoc :reason (or (get-in snapshot [:verification-state :reason])
                       (get-in snapshot [:verification-state :cancel-code])
                       "cancelled"))

    (= "verification.done" event-type)
    (assoc :verified true)))

(defn- verification-event
  [snapshot]
  (when-let [event-type (verification-event-type snapshot)]
    {:event event-type
     :data (verification-event-data event-type snapshot)}))

(defn- verification-event-signature
  [{:keys [event data]}]
  [event
   (:verification-id data)
   (get-in data [:verification-state :kind])
   (get-in data [:verification-state :sas-state :kind])
   (get-in data [:verification-state :sas-state :sas-emojis])
   (get-in data [:verification-state :cancel-code])
   (get-in data [:verification-state :reason])])

(defn- publish-verification-snapshot!
  [publish! seen* snapshot]
  (when-let [event (verification-event snapshot)]
    (let [verification-id (get-in event [:data :verification-id])
          signature (verification-event-signature event)]
      (when-not (= signature (get @seen* verification-id))
        (swap! seen* assoc verification-id signature)
        (publish! event)))))

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
             (activate-room-verification-request! client ev)
             (when-let [normalized (normalized-event ev bot-user-id)]
               (publish! normalized))
             nil)
           nil
           (room/get-timeline-events-from-now-on
            client
            {::mx/decryption-timeout (Duration/ofSeconds 8)}))))))))

(defn- publish-verification-emission!
  [publish! seen* emission]
  (cond
    (nil? emission) nil

    (or (map? emission) (instance? java.util.Map emission))
    (publish-verification-snapshot! publish! seen* (wire-data emission))

    (or (sequential? emission) (instance? java.lang.Iterable emission))
    (doseq [snapshot emission]
      (publish-verification-snapshot! publish! seen* (wire-data snapshot)))

    :else nil))

(defn- start-verification-flow!
  [flow publish! seen*]
  (start-virtual-thread!
   (fn []
     (m/?
      (m/reduce
       (fn [_ emission]
         (publish-verification-emission! publish! seen* emission)
         nil)
       nil
       flow)))))

(defn- start-verification-loop!
  [client event-sink]
  (when-let [publish! (:publish! event-sink)]
    (let [seen* (atom {})]
      {:device (start-verification-flow! (verification/active-device-verification client) publish! seen*)
       :users (start-verification-flow! (verification/active-user-verifications client) publish! seen*)})))

(def ^:private direct-invite-join-timeout (Duration/ofSeconds 15))

(defn- direct-invite?
  [room-snapshot]
  (and (= "invite" (some-> (::mx/membership room-snapshot) name))
       (true? (::mx/is-direct room-snapshot))))

(defn- join-direct-invite!
  [client room-snapshot]
  (when (direct-invite? room-snapshot)
    (let [room-id (::mx/room-id room-snapshot)]
      (m/? (room/join-room client room-id {::mx/timeout direct-invite-join-timeout}))
      {:room/id room-id
       :joined? true
       :activated (activate-recent-room-verification-requests! client room-id)})))

(defn- start-direct-invite-loop!
  [client]
  (let [seen* (atom #{})]
    (start-virtual-thread!
     (fn []
       (m/?
        (m/reduce
         (fn [_ rooms]
           (doseq [room-snapshot (or rooms [])]
             (let [room-id (::mx/room-id room-snapshot)]
               (when (and room-id
                          (direct-invite? room-snapshot)
                          (not (contains? @seen* room-id)))
                 (swap! seen* conj room-id)
                 (try
                   (join-direct-invite! client room-snapshot)
                   (catch Throwable ex
                     (tap> {:event :matrix/direct-invite-join-failed
                            :room/id room-id
                            :error (ex-message ex)}))))))
           nil)
         nil
         (room/get-all-flat client)))))))

(def ^:private default-space-key "default")
(def ^:private space-join-timeout (Duration/ofSeconds 15))
(def ^:private space-child-timeout (Duration/ofSeconds 10))

(defn- nonblank-string
  [value]
  (let [trimmed (str/trim (str value))]
    (when-not (str/blank? trimmed)
      trimmed)))

(defn- keyword-mode
  [mode]
  (cond
    (keyword? mode) mode
    (string? mode) (keyword mode)
    :else mode))

(defn- configured-space
  [config]
  (let [space (get-in config [:matrix :space])
        mode (keyword-mode (:mode space))]
    (when (and (:enabled? space) (contains? #{:existing :create} mode))
      (assoc space :mode mode))))

(defn- bot-user-id
  [runtime* config]
  (str (or (:user-id @runtime*)
           (get-in config [:matrix :user-id]))))

(defn- operators
  [config]
  (vec (get-in config [:matrix :operators] [])))

(defn- space-setup-ex
  ([message details]
   (space-setup-ex message details nil))
  ([message details cause]
   (ex-info message (merge {:code :matrix_space_setup_failed} details) cause)))

(defn- membership-join?
  [membership]
  (= "join" (some-> membership name)))

(defn- known-joined-space?
  [c space-id]
  (let [spaces (or (flow-value (space/get-all-flat c)) [])]
    (boolean
     (some (fn [candidate]
             (and (= space-id (str (::mx/room-id candidate)))
                  (membership-join? (::mx/membership candidate))))
           spaces))))

(defn- required-child-level
  [content]
  (long (or (get-in content [::mx/event-levels "m.space.child"])
            (::mx/state-default-level content)
            50)))

(defn- bot-level
  [content bot]
  (long (or (get-in content [::mx/user-levels bot])
            (::mx/users-default-level content)
            0)))

(defn- validate-space-manageable!
  [c config runtime* space-id]
  (when-not (known-joined-space? c space-id)
    (throw (space-setup-ex
            (str "Configured Matrix space " space-id " is not a joined Matrix Space for the bot. "
                 "Invite the bot and ensure the room is a Matrix Space.")
            {:space-id space-id
             :bot-user-id (bot-user-id runtime* config)})))
  (let [content (power-level-content-for-set (flow-value (room/get-power-levels c space-id)))
        bot (bot-user-id runtime* config)
        required (required-child-level content)
        actual (bot-level content bot)]
    (when (< actual required)
      (throw (space-setup-ex
              (str "Configured Matrix space " space-id " is not manageable by the bot. "
                   "Please invite/promote " bot " so it can manage m.space.child state events.")
              {:space-id space-id
               :bot-user-id bot
               :bot-level actual
               :required-level required}))))
  space-id)

(defn- resolve-existing-space-id!
  [c room-id-or-alias]
  (try
    (str (m/? (room/join-room c room-id-or-alias {::mx/timeout space-join-timeout})))
    (catch Throwable cause
      (throw (space-setup-ex
              (str "Could not join or resolve configured Matrix space " room-id-or-alias
                   ". Invite the bot to the space before setup, or use a room id/alias it can join.")
              {:room-id-or-alias room-id-or-alias}
              cause)))))

(defn- space-admin-power-levels
  [config runtime*]
  {::mx/user-levels (into {(bot-user-id runtime* config) 100}
                          (map (fn [operator]
                                 [operator 100]))
                          (operators config))})

(defn- create-space-opts
  [config runtime* space-config]
  (cond-> {::mx/room-name (or (nonblank-string (:name space-config)) "pi-matrix-relay")
           ::mx/visibility :private
           ::mx/preset :private-chat
           ::mx/invite (operators config)
           ::mx/power-levels (space-admin-power-levels config runtime*)}
    (empty? (operators config)) (dissoc ::mx/invite)))

(defn- create-and-remember-space!
  [c config runtime* db-conn space-config]
  (when-not db-conn
    (throw (space-setup-ex
            "Matrix Space creation requires the broker database so the created space can be reused on restart."
            {:space-mode :create})))
  (let [room-id (str (m/? (space/create-space c (create-space-opts config runtime* space-config))))]
    (store/remember-matrix-space! db-conn {:space-key default-space-key
                                           :room-id room-id
                                           :source :created
                                           :now-ms (System/currentTimeMillis)})
    room-id))

(defn- ensure-configured-space!
  [{:keys [config runtime*]} {:keys [db-conn]}]
  (if-let [space-config (configured-space config)]
    (let [c (:client @runtime*)
          mode (:mode space-config)]
      (when-not c
        (throw (space-setup-ex "Matrix client must be started before Matrix Space setup."
                               {:space-mode mode})))
      (if-let [room-id (:space/id @runtime*)]
        {:space/id room-id
         :space/mode (:space/mode @runtime*)}
        (let [room-id (case mode
                        :existing (resolve-existing-space-id! c (:room-id-or-alias space-config))
                        :create (or (:room-id (when db-conn
                                                (store/matrix-space @db-conn default-space-key)))
                                    (create-and-remember-space! c config runtime* db-conn space-config)))]
          (validate-space-manageable! c config runtime* room-id)
          (swap! runtime* assoc :space/id room-id :space/mode mode)
          {:space/id room-id
           :space/mode mode})))
    {:space/enabled? false}))

(defn- child-via
  [room-id]
  (if-let [server (second (str/split (str room-id) #":" 2))]
    #{server}
    (throw (matrix/ex :invalid_request
                      "Matrix room IDs must include a server name for Matrix Space child links."
                      {:room/id room-id}))))

(defn- ensure-room-linked-to-space!
  [{:keys [runtime*]} request]
  (if-let [space-id (:space/id @runtime*)]
    (let [c (:client @runtime*)
          room-id (:room/id request)]
      (m/? (space/set-child c space-id room-id {::mx/via (child-via room-id)} {::mx/timeout space-child-timeout}))
      {:space/id space-id
       :room/id room-id
       :linked? true})
    {:room/id (:room/id request)
     :linked? false}))

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
                            :verification-loop (start-verification-loop! opened event-sink)
                            :direct-invite-loop (start-direct-invite-loop! opened)
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
    (let [c (:client @runtime*)
          room-id (:room/id request)
          level (long (or (:level request) 100))
          users (vec (:users request))
          content (power-level-content-for-set (flow-value (room/get-power-levels c room-id)))
          current-users (or (::mx/user-levels content) {})
          updated-content (assoc content ::mx/user-levels (reduce #(assoc %1 %2 level) current-users users))]
      (when (not= content updated-content)
        (m/? (room/set-power-levels c room-id updated-content {::mx/timeout (Duration/ofSeconds 10)})))
      {:room/id room-id
       :users users
       :level level}))
  (ensure-space! [this request]
    (ensure-configured-space! this request))
  (ensure-room-in-space! [this request]
    (ensure-room-linked-to-space! this request))
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
    (let [c (:client @runtime*)
          user-id (:user/id request)]
      (if-let [device-id (:device/id request)]
        (verification-task-result (verification/start-device-verification! c user-id device-id))
        (verification-task-result (verification/start-user-verification! c user-id)))))
  (verification-bootstrap! [_ request]
    (let [c (:client @runtime*)
          password (get-in config [:matrix :password])
          user-id (bot-user-id runtime* config)
          opts (cond-> {::mx/password password
                         ::mx/user-id user-id}
                 (str/blank? (str password)) (dissoc ::mx/password)
                 (str/blank? user-id) (dissoc ::mx/user-id))]
      (if-let [passphrase (:passphrase request)]
        (verification-task-result (key/bootstrap-cross-signing-from-passphrase! c passphrase opts))
        (verification-task-result (key/bootstrap-cross-signing! c opts)))))
  (verification-accept! [_ verification-id]
    (verification-task-result (verification/accept! (:client @runtime*) verification-id)))
  (verification-start-sas! [_ verification-id]
    (verification-task-result (verification/start-sas! (:client @runtime*) verification-id)))
  (verification-confirm! [_ verification-id]
    (verification-task-result (verification/confirm! (:client @runtime*) verification-id)))
  (verification-no-match! [_ verification-id]
    (verification-task-result (verification/no-match! (:client @runtime*) verification-id)))
  (verification-cancel! [_ verification-id]
    (verification-task-result (verification/cancel! (:client @runtime*) verification-id nil)))
  (verification-status [_]
    {:verifications (verification-snapshots (:client @runtime*))})
  (verification-targets [_]
    {:targets (joined-dm-verification-targets (:client @runtime*)
                                               (bot-user-id runtime* config))}))

(defn gateway
  ([config paths]
   (gateway config paths nil))
  ([config paths event-sink]
   (->TrixnityGateway config paths event-sink (atom {}))))
