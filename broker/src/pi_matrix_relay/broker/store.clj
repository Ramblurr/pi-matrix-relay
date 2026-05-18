(ns pi-matrix-relay.broker.store
  (:require [clojure.string :as str]
            [datahike.api :as d]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.slots :as slots])
  (:import [java.security MessageDigest]
           [java.util Date]))

(def active-lease-states #{:reserved :leased :suspect})
(def terminal-lease-states #{:released :failed})
(def allowed-delivery-modes #{:follow-up :steer :reject})
(def allowed-prompt-modes #{:all :mentions :commands-only})
(def idempotency-result-byte-limit 2048)

(defn- now-date
  [now-ms]
  (Date. (long (or now-ms (System/currentTimeMillis)))))

(defn- instant->ms
  [x]
  (when x
    (.getTime ^Date x)))

(defn- parse-json-map
  [s]
  (when (seq s)
    (json/read-json s)))

(defn- write-json
  [x]
  (when (some? x)
    (json/write-json x)))

(defn- project-key
  [project]
  (or (:project/id project) (:project/key project)))

(defn- project-root
  [project]
  (:project/root project))

(defn- project-display-name
  [project]
  (or (:project/display-name project) (:display-name project)))

(defn- first-entity
  [db query & inputs]
  (->> (apply d/q query db inputs)
       (map first)
       sort
       first))

(defn- project-from-entity
  [db entity-id]
  (when entity-id
    (let [project (d/pull db [:project/id :project/key :project/root :project/display-name]
                          entity-id)]
      (cond-> {:project-id (:project/id project)
               :project-key (:project/key project)}
        (:project/root project) (assoc :project-root (:project/root project))
        (:project/display-name project) (assoc :project-display-name (:project/display-name project))))))

(defn project-by-key
  [db key]
  (project-from-entity
   db
   (first-entity db
                 '[:find ?project
                   :in $ ?project-key
                   :where [?project :project/key ?project-key]]
                 key)))

(defn- room-from-entity
  [db entity-id]
  (when entity-id
    (let [room (d/pull db [:room/id :room/default-delivery-mode] entity-id)]
      {:room/id (:room/id room)
       :room/default-delivery-mode (:room/default-delivery-mode room)})))

(defn room-by-id
  [db room-id]
  (room-from-entity
   db
   (first-entity db
                 '[:find ?room
                   :in $ ?room-id
                   :where [?room :room/id ?room-id]]
                 room-id)))

(defn ensure-room!
  [conn room-id]
  (when (str/blank? (str room-id))
    (throw (ex-info "Matrix room ID is required." {:code :invalid_request})))
  (d/transact conn [{:room/id room-id}])
  (room-by-id @conn room-id))

(defn ensure-project!
  [conn project]
  (let [key (project-key project)]
    (when-not (seq key)
      (throw (ex-info "Project id is required." {:code :invalid_request})))
    (or (project-by-key @conn key)
        (let [project-id (random-uuid)
              tx (cond-> {:project/id project-id
                          :project/key key}
                   (project-root project) (assoc :project/root (project-root project))
                   (project-display-name project) (assoc :project/display-name (project-display-name project)))]
          (try
            (d/transact conn [tx])
            (catch clojure.lang.ExceptionInfo ex
              (if (project-by-key @conn key)
                nil
                (throw ex))))
          (project-by-key @conn key)))))

(defn- client-from-entity
  [db entity-id]
  (when entity-id
    (let [client (d/pull db [:client/instance-id
                             :client/protocol-version
                             :client/metadata-json
                             :client/registered-at
                             :client/last-heartbeat-at
                             :client/state
                             {:client/subscribed-room [:room/id]}
                             {:client/project [:project/id :project/key :project/root :project/display-name]}]
                         entity-id)]
      {:client-id (:client/instance-id client)
       :protocol-version (:client/protocol-version client)
       :project {:project-id (get-in client [:client/project :project/id])
                 :project-key (get-in client [:client/project :project/key])
                 :project-root (get-in client [:client/project :project/root])
                 :project-display-name (get-in client [:client/project :project/display-name])}
       :metadata (parse-json-map (:client/metadata-json client))
       :subscriptions (set (keep :room/id (:client/subscribed-room client)))
       :registered-at (instant->ms (:client/registered-at client))
       :last-heartbeat-at (instant->ms (:client/last-heartbeat-at client))
       :state (:client/state client)})))

(defn client
  [db client-id]
  (client-from-entity
   db
   (first-entity db
                 '[:find ?client
                   :in $ ?client-id
                   :where [?client :client/instance-id ?client-id]]
                 client-id)))

(defn require-client
  [db client-id]
  (or (client db client-id)
      (throw (ex-info "Unknown broker client." {:code :client_not_found
                                                :client-id client-id}))))

(defn client-count
  [db]
  (count (d/q '[:find ?client
                :where [?client :client/instance-id _]]
              db)))

(defn- subscribed-room-ids
  [db client-id]
  (set (d/q '[:find [?room-id ...]
              :in $ ?client-id
              :where
              [?client :client/instance-id ?client-id]
              [?client :client/subscribed-room ?room]
              [?room :room/id ?room-id]]
            db client-id)))

(defn- set-client-subscriptions!
  [conn client-id rooms]
  (require-client @conn client-id)
  (let [rooms (set (or rooms []))
        _ (doseq [room rooms]
            (ensure-room! conn room))
        old (subscribed-room-ids @conn client-id)
        tx-data (concat
                 (for [room (remove rooms old)]
                   [:db/retract [:client/instance-id client-id] :client/subscribed-room [:room/id room]])
                 (for [room (remove old rooms)]
                   [:db/add [:client/instance-id client-id] :client/subscribed-room [:room/id room]]))]
    (when (seq tx-data)
      (d/transact conn (vec tx-data)))
    {:rooms (vec rooms)}))

(defn register-client!
  [conn {:keys [now-ms heartbeat-seconds global-operators]} request]
  (let [client-id (:client/instance-id request)
        project (:project request)
        project (ensure-project! conn project)
        now (now-date now-ms)]
    (when-not (seq client-id)
      (throw (ex-info "Client instance id is required." {:code :invalid_request})))
    (d/transact
     conn
     [(cond-> {:client/instance-id client-id
               :client/protocol-version (:protocol/version request)
               :client/project [:project/id (:project-id project)]
               :client/registered-at now
               :client/last-heartbeat-at now
               :client/state :registered}
        (:metadata request) (assoc :client/metadata-json (write-json (:metadata request))))])
    (set-client-subscriptions! conn client-id (get-in request [:subscriptions :rooms]))
    {:client-id client-id
     :heartbeat-seconds (or heartbeat-seconds 30)
     :global-operators (vec (or global-operators []))}))

(defn update-subscriptions!
  [conn client-id rooms]
  (set-client-subscriptions! conn client-id rooms))

(defn- lease-from-entity
  [db entity-id]
  (when entity-id
    (let [lease (d/pull db [:lease/id
                            :lease/slot
                            :lease/state
                            :lease/reservation-id
                            :lease/reserved-at
                            :lease/acquired-at
                            :lease/last-heartbeat-at
                            :lease/suspect-at
                            :lease/released-at
                            :lease/release-reason
                            {:lease/project [:project/id :project/key]}
                            {:lease/client [:client/instance-id :client/metadata-json]}
                            {:lease/slot-room [:slot-room/name {:slot-room/room [:room/id]}]}]
                        entity-id)]
      (cond-> {:lease-id (:lease/id lease)
               :slot (:lease/slot lease)
               :state (:lease/state lease)
               :reservation-id (:lease/reservation-id lease)
               :project-id (get-in lease [:lease/project :project/id])
               :project-key (get-in lease [:lease/project :project/key])
               :client-id (get-in lease [:lease/client :client/instance-id])
               :client-metadata (parse-json-map (get-in lease [:lease/client :client/metadata-json]))
               :reserved-at (instant->ms (:lease/reserved-at lease))
               :acquired-at (instant->ms (:lease/acquired-at lease))
               :last-heartbeat-at (instant->ms (:lease/last-heartbeat-at lease))
               :suspect-at (instant->ms (:lease/suspect-at lease))
               :released-at (instant->ms (:lease/released-at lease))}
        (get-in lease [:lease/slot-room :slot-room/room :room/id])
        (assoc :room-id (get-in lease [:lease/slot-room :slot-room/room :room/id]))
        (get-in lease [:lease/slot-room :slot-room/name])
        (assoc :room-name (get-in lease [:lease/slot-room :slot-room/name]))
        (:lease/release-reason lease)
        (assoc :release-reason (:lease/release-reason lease))))))

(defn lease-by-id
  [db lease-id]
  (lease-from-entity
   db
   (first-entity db
                 '[:find ?lease
                   :in $ ?lease-id
                   :where [?lease :lease/id ?lease-id]]
                 lease-id)))

(defn- active-slots-for-project-id
  [db project-id]
  (set (d/q '[:find [?slot ...]
              :in $ ?project-id
              :where
              [?project :project/id ?project-id]
              [?lease :lease/project ?project]
              [?lease :lease/state ?state]
              [(contains? #{:reserved :leased :suspect} ?state)]
              [?lease :lease/slot ?slot]]
            db project-id)))

(defn- project-row
  [db key]
  (first (sort-by first
                  (d/q '[:find ?project ?project-id
                         :in $ ?project-key
                         :where
                         [?project :project/key ?project-key]
                         [?project :project/id ?project-id]]
                       db key))))

(defn reserve-slot-tx
  [db {:keys [project-key project-root project-display-name project-id client-id lease-id reservation-id now]}]
  (let [[project-eid existing-project-id] (project-row db project-key)
        project-id (or existing-project-id project-id)
        active-slots (active-slots-for-project-id db project-id)
        slot (slots/first-free-slot active-slots)
        project-ref [:project/id project-id]]
    (cond-> []
      (nil? project-eid)
      (conj (cond-> {:project/id project-id
                     :project/key project-key}
              project-root (assoc :project/root project-root)
              project-display-name (assoc :project/display-name project-display-name)))

      true
      (conj {:lease/id lease-id
             :lease/project project-ref
             :lease/client [:client/instance-id client-id]
             :lease/slot slot
             :lease/state :reserved
             :lease/reservation-id reservation-id
             :lease/reserved-at now
             :lease/last-heartbeat-at now}))))

(defn reserve-slot!
  [conn {:keys [now-ms]} {:keys [client-id project]}]
  (let [project-key (project-key project)
        lease-id (random-uuid)
        reservation-id (random-uuid)
        now (now-date now-ms)]
    (when-not (seq project-key)
      (throw (ex-info "Project id is required to reserve a slot." {:code :invalid_request})))
    (require-client @conn client-id)
    (d/transact
     conn
     [[:db.fn/call reserve-slot-tx
       {:project-key project-key
        :project-root (project-root project)
        :project-display-name (project-display-name project)
        :project-id (random-uuid)
        :client-id client-id
        :lease-id lease-id
        :reservation-id reservation-id
        :now now}]])
    (lease-by-id @conn lease-id)))

(defn- slot-room-from-entity
  [db entity-id]
  (when entity-id
    (let [room (d/pull db [:slot-room/slot
                           {:slot-room/room [:room/id]}
                           :slot-room/name
                           :slot-room/created-at
                           :slot-room/updated-at
                           {:slot-room/project [:project/id :project/key]}]
                       entity-id)]
      {:slot (:slot-room/slot room)
       :room-id (get-in room [:slot-room/room :room/id])
       :room-name (:slot-room/name room)
       :project-id (get-in room [:slot-room/project :project/id])
       :project-key (get-in room [:slot-room/project :project/key])
       :created-at (instant->ms (:slot-room/created-at room))
       :updated-at (instant->ms (:slot-room/updated-at room))})))

(defn- slot-room-eid
  [db project-key slot]
  (first-entity db
                '[:find ?room
                  :in $ ?project-key ?slot
                  :where
                  [?project :project/key ?project-key]
                  [?room :slot-room/project ?project]
                  [?room :slot-room/slot ?slot]]
                project-key slot))

(defn slot-room
  [db project-key slot]
  (slot-room-from-entity db (slot-room-eid db project-key slot)))

(defn- slot-room-eid-by-room-id
  [db room-id]
  (first-entity db
                '[:find ?slot-room
                  :in $ ?room-id
                  :where
                  [?room :room/id ?room-id]
                  [?slot-room :slot-room/room ?room]]
                room-id))

(defn- slot-room-by-room-id
  [db room-id]
  (slot-room-from-entity db (slot-room-eid-by-room-id db room-id)))

(defn remember-slot-room!
  [conn {:keys [project slot room-id room-name now-ms] :as command}]
  (let [project-key (or (:project-key command) (project-key project))
        project (ensure-project! conn (or project {:project/id project-key}))
        now (now-date now-ms)
        _ (when-not (seq slot)
            (throw (ex-info "Slot is required." {:code :invalid_request})))
        _ (ensure-room! conn room-id)
        existing (or (slot-room @conn project-key slot)
                     (slot-room-by-room-id @conn room-id))]
    (if existing
      existing
      (do
        (d/transact conn [{:slot-room/project [:project/id (:project-id project)]
                           :slot-room/slot slot
                           :slot-room/room [:room/id room-id]
                           :slot-room/name room-name
                           :slot-room/created-at now
                           :slot-room/updated-at now}])
        (slot-room @conn project-key slot)))))

(defn complete-slot-reservation!
  [conn {:keys [now-ms lease-id reservation-id client-id room-id room-name]}]
  (let [now (now-date now-ms)
        lease (lease-by-id @conn lease-id)]
    (when-not (and lease
                   (= :reserved (:state lease))
                   (= reservation-id (:reservation-id lease))
                   (= client-id (:client-id lease)))
      (throw (ex-info "Slot reservation is no longer active."
                      {:code :slot_not_found
                       :client-id client-id
                       :slot (:slot lease)})))
    (ensure-room! conn room-id)
    (let [slot-room-eid (slot-room-eid-by-room-id @conn room-id)
          slot-room-ref (or slot-room-eid -1)]
      (d/transact
       conn
       [[:db/cas [:lease/id lease-id] :lease/state :reserved :leased]
        (cond-> {:db/id slot-room-ref
                 :slot-room/project [:project/id (:project-id lease)]
                 :slot-room/slot (:slot lease)
                 :slot-room/room [:room/id room-id]
                 :slot-room/name room-name
                 :slot-room/updated-at now}
          (nil? slot-room-eid) (assoc :slot-room/created-at now))
        [:db/add [:lease/id lease-id] :lease/slot-room slot-room-ref]
        [:db/add [:lease/id lease-id] :lease/acquired-at now]
        [:db/add [:lease/id lease-id] :lease/last-heartbeat-at now]]))
    (lease-by-id @conn lease-id)))

(defn abandon-slot-reservation!
  [conn {:keys [now-ms lease-id reservation-id client-id]}]
  (let [now (now-date now-ms)
        lease (lease-by-id @conn lease-id)]
    (if (and lease
             (= :reserved (:state lease))
             (= reservation-id (:reservation-id lease))
             (= client-id (:client-id lease)))
      (do
        (d/transact conn [[:db/cas [:lease/id lease-id] :lease/state :reserved :failed]
                          [:db/add [:lease/id lease-id] :lease/released-at now]
                          [:db/add [:lease/id lease-id] :lease/release-reason :abandoned]])
        {:released true})
      {:released false})))

(defn- lease-time
  [lease]
  (or (:acquired-at lease)
      (:reserved-at lease)
      (:released-at lease)
      0))

(defn- latest-visible-leases
  [leases]
  (->> leases
       (remove #(= :failed (:state %)))
       (group-by :slot)
       vals
       (map #(last (sort-by lease-time %)))
       (sort-by :slot)
       vec))

(defn list-slots
  [db project-key]
  {:project-key project-key
   :slots (latest-visible-leases
           (mapv #(lease-from-entity db %)
                 (d/q '[:find [?lease ...]
                        :in $ ?project-key
                        :where
                        [?project :project/key ?project-key]
                        [?lease :lease/project ?project]
                        [?lease :lease/slot _]]
                      db project-key)))})

(defn- active-leases-for-client
  [db client-id]
  (mapv #(lease-from-entity db %)
        (d/q '[:find [?lease ...]
               :in $ ?client-id
               :where
               [?client :client/instance-id ?client-id]
               [?lease :lease/client ?client]
               [?lease :lease/state ?state]
               [(contains? #{:reserved :leased :suspect} ?state)]]
             db client-id)))

(defn- active-lease-for-client
  [db {:keys [client-id room-id slot]}]
  (first (filter (fn [lease]
                   (and (or (nil? room-id) (= room-id (:room-id lease)))
                        (or (nil? slot) (= slot (:slot lease)))))
                 (active-leases-for-client db client-id))))

(defn release-slot!
  [conn {:keys [now-ms client-id room-id slot reason]}]
  (require-client @conn client-id)
  (if-let [lease (active-lease-for-client @conn {:client-id client-id :room-id room-id :slot slot})]
    (let [now (now-date now-ms)]
      (d/transact conn [[:db/cas [:lease/id (:lease-id lease)] :lease/state (:state lease) :released]
                        [:db/add [:lease/id (:lease-id lease)] :lease/released-at now]
                        [:db/add [:lease/id (:lease-id lease)] :lease/release-reason (or reason :released)]])
      {:released true})
    {:released false}))

(defn heartbeat!
  [conn client-id now-ms]
  (require-client @conn client-id)
  (let [now (now-date now-ms)
        leases (active-leases-for-client @conn client-id)
        tx-data (into [[:db/add [:client/instance-id client-id] :client/last-heartbeat-at now]
                       [:db/add [:client/instance-id client-id] :client/state :registered]]
                      (mapcat (fn [{:keys [lease-id state]}]
                                (cond-> [[:db/add [:lease/id lease-id] :lease/last-heartbeat-at now]]
                                  (= :suspect state)
                                  (conj [:db/cas [:lease/id lease-id] :lease/state :suspect :leased])))
                              leases))]
    (d/transact conn tx-data)
    {:heartbeat-seconds 30}))

(defn unregister-client!
  [conn client-id now-ms]
  (require-client @conn client-id)
  (let [now (now-date now-ms)
        leases (active-leases-for-client @conn client-id)
        tx-data (into [[:db/add [:client/instance-id client-id] :client/state :unregistered]]
                      (mapcat (fn [{:keys [lease-id state]}]
                                [[:db/cas [:lease/id lease-id] :lease/state state :released]
                                 [:db/add [:lease/id lease-id] :lease/released-at now]
                                 [:db/add [:lease/id lease-id] :lease/release-reason :unregister]])
                              leases))]
    (d/transact conn tx-data)
    {}))

(defn mark-client-suspect!
  [conn client-id now-ms]
  (require-client @conn client-id)
  (let [now (now-date now-ms)]
    (doseq [{:keys [lease-id state]} (active-leases-for-client @conn client-id)]
      (when (and (active-lease-states state)
                 (not= :suspect state))
        (try
          (d/transact conn [[:db/cas [:lease/id lease-id] :lease/state state :suspect]
                            [:db/add [:lease/id lease-id] :lease/suspect-at now]])
          (catch clojure.lang.ExceptionInfo _
            nil))))
    nil))

(defn- active-leases
  [db]
  (mapv #(lease-from-entity db %)
        (d/q '[:find [?lease ...]
               :where
               [?lease :lease/state ?state]
               [(contains? #{:reserved :leased :suspect} ?state)]]
             db)))

(defn mark-stale-leases!
  ([conn now-ms heartbeat-seconds stale-after-missed]
   (mark-stale-leases! conn now-ms heartbeat-seconds stale-after-missed {}))
  ([conn now-ms heartbeat-seconds stale-after-missed {:keys [exclude-client-ids]}]
   (let [exclude-client-ids (set exclude-client-ids)
         cutoff (- now-ms (* heartbeat-seconds stale-after-missed 1000))
         stale (->> (active-leases @conn)
                    (remove #(contains? exclude-client-ids (:client-id %)))
                    (filter #(< (or (:last-heartbeat-at %) 0) cutoff))
                    vec)
         now (now-date now-ms)]
     (doseq [{:keys [lease-id state]} stale]
       (try
         (d/transact conn [[:db/cas [:lease/id lease-id] :lease/state state :released]
                           [:db/add [:lease/id lease-id] :lease/released-at now]
                           [:db/add [:lease/id lease-id] :lease/release-reason :stale]])
         (catch clojure.lang.ExceptionInfo _
           nil)))
     stale)))

(defn known-room-for-client?
  [db client-id room-id]
  (require-client db client-id)
  (boolean
   (or (seq (d/q '[:find [?room ...]
                   :in $ ?client-id ?room-id
                   :where
                   [?client :client/instance-id ?client-id]
                   [?room :room/id ?room-id]
                   [?client :client/subscribed-room ?room]]
                 db client-id room-id))
       (seq (d/q '[:find [?lease ...]
                   :in $ ?client-id ?room-id
                   :where
                   [?client :client/instance-id ?client-id]
                   [?room :room/id ?room-id]
                   [?slot-room :slot-room/room ?room]
                   [?lease :lease/slot-room ?slot-room]
                   [?lease :lease/client ?client]
                   [?lease :lease/state ?lease-state]
                   [(contains? #{:reserved :leased :suspect} ?lease-state)]]
                 db client-id room-id)))))

(defn- normalize-delivery-mode
  [mode]
  (cond
    (keyword? mode) mode
    (string? mode) (keyword mode)
    :else mode))

(defn- validate-delivery-mode!
  [mode]
  (let [mode (normalize-delivery-mode mode)]
    (when-not (contains? allowed-delivery-modes mode)
      (throw (ex-info "Invalid room delivery mode."
                      {:code :invalid_request
                       :default-delivery-mode (if (keyword? mode) (name mode) mode)
                       :allowed (mapv name (sort allowed-delivery-modes))})))
    mode))

(defn room-default-delivery-mode
  [db room-id]
  (:room/default-delivery-mode (room-by-id db room-id)))

(defn room-delivery-mode
  [db room-id]
  (let [entity-id (first-entity db
                                '[:find ?room
                                  :in $ ?room-id
                                  :where [?room :room/id ?room-id]]
                                room-id)]
    (if entity-id
      (let [room (d/pull db [:room/id
                             :room/default-delivery-mode
                             :room/default-delivery-mode-updated-at
                             :room/default-delivery-mode-updated-by-user
                             {:room/default-delivery-mode-updated-by-client [:client/instance-id]}]
                         entity-id)]
        {:room-id (:room/id room)
         :default-delivery-mode (:room/default-delivery-mode room)
         :updated-at (instant->ms (:room/default-delivery-mode-updated-at room))
         :updated-by-client (get-in room [:room/default-delivery-mode-updated-by-client :client/instance-id])
         :updated-by-user (:room/default-delivery-mode-updated-by-user room)})
      {:room-id room-id
       :default-delivery-mode nil
       :updated-at nil
       :updated-by-client nil
       :updated-by-user nil})))

(defn set-room-default-delivery-mode!
  [conn {:keys [client-id room-id default-delivery-mode updated-by-user now-ms]}]
  (let [mode (validate-delivery-mode! default-delivery-mode)
        now (now-date now-ms)]
    (when-not (known-room-for-client? @conn client-id room-id)
      (throw (ex-info "Client is not registered for the target Matrix room."
                      {:code :room_not_allowed
                       :client-id client-id
                       :room-id room-id})))
    (ensure-room! conn room-id)
    (d/transact conn (cond-> [[:db/add [:room/id room-id] :room/default-delivery-mode mode]
                              [:db/add [:room/id room-id] :room/default-delivery-mode-updated-at now]
                              [:db/add [:room/id room-id] :room/default-delivery-mode-updated-by-client [:client/instance-id client-id]]]
                       (some? updated-by-user)
                       (conj [:db/add [:room/id room-id] :room/default-delivery-mode-updated-by-user updated-by-user])))
    (room-delivery-mode @conn room-id)))

(defn- normalize-prompt-mode
  [mode]
  (let [mode (cond
               (= "mention" mode) "mentions"
               :else mode)]
    (cond
      (keyword? mode) (if (= :mention mode) :mentions mode)
      (string? mode) (keyword mode)
      :else mode)))

(defn- validate-prompt-mode!
  [mode]
  (let [mode (normalize-prompt-mode mode)]
    (when-not (contains? allowed-prompt-modes mode)
      (throw (ex-info "Invalid prompt mode."
                      {:code :invalid_request
                       :room/prompt-mode (if (keyword? mode) (name mode) mode)
                       :allowed (mapv name (sort allowed-prompt-modes))})))
    mode))

(defn room-prompt-mode
  [db room-id]
  (let [entity-id (first-entity db
                                '[:find ?room
                                  :in $ ?room-id
                                  :where [?room :room/id ?room-id]]
                                room-id)]
    (if entity-id
      (let [room (d/pull db [:room/id
                             :room/prompt-mode
                             :room/prompt-mode-updated-at
                             :room/prompt-mode-updated-by-user
                             {:room/prompt-mode-updated-by-client [:client/instance-id]}]
                         entity-id)]
        {:room-id (:room/id room)
         :mode (:room/prompt-mode room)
         :updated-at (instant->ms (:room/prompt-mode-updated-at room))
         :updated-by-client (get-in room [:room/prompt-mode-updated-by-client :client/instance-id])
         :updated-by-user (:room/prompt-mode-updated-by-user room)})
      {:room-id room-id
       :mode nil
       :updated-at nil
       :updated-by-client nil
       :updated-by-user nil})))

(defn set-room-prompt-mode!
  [conn {:keys [client-id room-id mode updated-by-user now-ms]}]
  (let [mode (validate-prompt-mode! mode)
        now (now-date now-ms)]
    (when-not (known-room-for-client? @conn client-id room-id)
      (throw (ex-info "Client is not registered for the target Matrix room."
                      {:code :room_not_allowed
                       :client-id client-id
                       :room-id room-id})))
    (ensure-room! conn room-id)
    (d/transact conn (cond-> [[:db/add [:room/id room-id] :room/prompt-mode mode]
                              [:db/add [:room/id room-id] :room/prompt-mode-updated-at now]
                              [:db/add [:room/id room-id] :room/prompt-mode-updated-by-client [:client/instance-id client-id]]]
                       (some? updated-by-user)
                       (conj [:db/add [:room/id room-id] :room/prompt-mode-updated-by-user updated-by-user])))
    (room-prompt-mode @conn room-id)))

(defn clients-for-room
  [db room-id]
  (let [client-ids (if room-id
                     (into #{}
                           (concat
                            (d/q '[:find [?client-id ...]
                                   :in $ ?room-id
                                   :where
                                   [?client :client/instance-id ?client-id]
                                   [?client :client/state ?state]
                                   [(not= ?state :unregistered)]
                                   [?room :room/id ?room-id]
                                   [?client :client/subscribed-room ?room]]
                                 db room-id)
                            (d/q '[:find [?client-id ...]
                                   :in $ ?room-id
                                   :where
                                   [?room :room/id ?room-id]
                                   [?slot-room :slot-room/room ?room]
                                   [?lease :lease/slot-room ?slot-room]
                                   [?lease :lease/state ?lease-state]
                                   [(contains? #{:reserved :leased :suspect} ?lease-state)]
                                   [?lease :lease/client ?client]
                                   [?client :client/instance-id ?client-id]]
                                 db room-id)))
                     (set (d/q '[:find [?client-id ...]
                                 :where
                                 [?client :client/instance-id ?client-id]
                                 [?client :client/state ?state]
                                 [(not= ?state :unregistered)]]
                               db)))]
    (vec (sort client-ids))))

(defn- canonicalize
  [x]
  (cond
    (map? x) (into (sorted-map-by (fn [a b] (compare (pr-str a) (pr-str b))))
                   (map (fn [[k v]] [k (canonicalize v)]))
                   x)
    (sequential? x) (mapv canonicalize x)
    (set? x) (vec (sort-by pr-str (map canonicalize x)))
    :else x))

(defn sha256-hex
  [s]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (str s) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and % 0xff)) digest))))

(defn request-fingerprint
  [operation payload]
  (sha256-hex (pr-str (canonicalize {:operation operation
                                     :payload payload}))))

(defn request-tx
  [db {:keys [request-id operation fingerprint owner-id now expires-at]}]
  (if (first-entity db
                    '[:find ?request
                      :in $ ?request-id
                      :where [?request :request/id ?request-id]]
                    request-id)
    []
    [{:request/id request-id
      :request/fingerprint fingerprint
      :request/operation operation
      :request/status :pending
      :request/owner-id owner-id
      :request/created-at now
      :request/expires-at expires-at}]))

(defn- request-from-entity
  [db entity-id]
  (when entity-id
    (let [request (d/pull db [:request/id
                              :request/fingerprint
                              :request/operation
                              :request/status
                              :request/owner-id
                              :request/result-json
                              :request/error-code
                              :request/created-at
                              :request/completed-at
                              :request/expires-at]
                          entity-id)]
      (cond-> {:request-id (:request/id request)
               :fingerprint (:request/fingerprint request)
               :operation (:request/operation request)
               :status (:request/status request)
               :owner-id (:request/owner-id request)
               :created-at (instant->ms (:request/created-at request))
               :completed-at (instant->ms (:request/completed-at request))
               :expires-at (instant->ms (:request/expires-at request))}
        (:request/result-json request)
        (assoc :result (parse-json-map (:request/result-json request)))
        (:request/error-code request)
        (assoc :error-code (:request/error-code request))))))

(defn request-by-id
  [db request-id]
  (request-from-entity
   db
   (first-entity db
                 '[:find ?request
                   :in $ ?request-id
                   :where [?request :request/id ?request-id]]
                 request-id)))

(defn reserve-request!
  [conn {:keys [request-id operation fingerprint owner-id now-ms retry-after-ms]}]
  (let [owner-id (or owner-id (random-uuid))
        now (now-date now-ms)]
    (d/transact
     conn
     [[:db.fn/call request-tx
       {:request-id request-id
        :operation operation
        :fingerprint fingerprint
        :owner-id owner-id
        :now now
        :expires-at now}]])
    (let [record (request-by-id @conn request-id)]
      (cond
        (not= fingerprint (:fingerprint record))
        (throw (ex-info "Request id was reused with a different payload."
                        {:code :idempotency_conflict
                         :request-id request-id}))

        (= :completed (:status record))
        (select-keys record [:status :result])

        (and (= :pending (:status record))
             (= owner-id (:owner-id record)))
        {:status :reserved}

        (= :pending (:status record))
        {:status :pending
         :retry-after-ms (or retry-after-ms 1000)}

        :else
        record))))

(defn complete-request!
  [conn {:keys [request-id now-ms]} result]
  (let [result-json (json/write-json (canonicalize result))
        byte-count (alength (.getBytes result-json "UTF-8"))]
    (when (> byte-count idempotency-result-byte-limit)
      (throw (ex-info "Idempotency result JSON exceeds 2048 UTF-8 bytes."
                      {:code :idempotency_result_too_large
                       :request-id request-id
                       :byte-count byte-count
                       :limit idempotency-result-byte-limit})))
    (d/transact conn [[:db/cas [:request/id request-id] :request/status :pending :completed]
                      [:db/add [:request/id request-id] :request/result-json result-json]
                      [:db/add [:request/id request-id] :request/completed-at (now-date now-ms)]])
    (select-keys (request-by-id @conn request-id) [:status :result])))
