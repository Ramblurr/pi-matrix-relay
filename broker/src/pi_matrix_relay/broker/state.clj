(ns pi-matrix-relay.broker.state
  (:require [pi-matrix-relay.broker.slots :as slots]))

(defn empty-state
  []
  {:clients {}
   :slots {}
   :slot-rooms {}
   :events []
   :next-event-id 0
   :joined-rooms {}
   :sent-request-ids {}
   :verifications {}})

(defn now-ms [] (System/currentTimeMillis))

(defn random-id
  [prefix]
  (str prefix "-" (random-uuid)))

(defn register-client!
  [state* {:keys [client-id-fn now heartbeat-seconds global-operators]} request]
  (let [now (or now (now-ms))
        client-id ((or client-id-fn #(random-id "client")))
        rooms (set (get-in request [:subscriptions :rooms]))
        client {:client-id client-id
                :client-instance-id (:clientInstanceId request)
                :protocol-version (:protocolVersion request)
                :project (:project request)
                :metadata (:metadata request)
                :subscriptions rooms
                :acquired-rooms #{}
                :registered-at now
                :last-heartbeat-at now
                :state :registered}]
    (swap! state* assoc-in [:clients client-id] client)
    {:clientId client-id
     :eventStream (str "/v1/clients/" client-id "/events")
     :heartbeatSeconds heartbeat-seconds
     :globalOperators (vec global-operators)}))

(defn client
  [state client-id]
  (get-in state [:clients client-id]))

(defn require-client
  [state client-id]
  (or (client state client-id)
      (throw (ex-info "Unknown broker client." {:code :client_not_found
                                                :client-id client-id}))))

(defn update-subscriptions!
  [state* client-id rooms]
  (let [rooms (set rooms)]
    (swap! state*
           (fn [state]
             (require-client state client-id)
             (assoc-in state [:clients client-id :subscriptions] rooms)))
    {:rooms (vec rooms)}))

(defn heartbeat!
  [state* client-id now]
  (swap! state*
         (fn [state]
           (require-client state client-id)
           (-> state
               (assoc-in [:clients client-id :last-heartbeat-at] now)
               (update :slots
                       (fn [slots-by-project]
                         (into {}
                               (map (fn [[project-id leases]]
                                      [project-id
                                       (into {}
                                             (map (fn [[slot lease]]
                                                    [slot (cond-> lease
                                                            (= client-id (:client-id lease))
                                                            (assoc :last-heartbeat-at now
                                                                   :state :leased))]))
                                             leases)]))
                               slots-by-project))))))
  {:heartbeatSeconds 30})

(defn unregister-client!
  [state* client-id]
  (swap! state*
         (fn [state]
           (require-client state client-id)
           (update state :clients dissoc client-id)))
  {})

(defn known-room-for-client?
  [state client-id room-id]
  (let [client (require-client state client-id)]
    (boolean
     (or (contains? (:subscriptions client) room-id)
         (contains? (:acquired-rooms client) room-id)
         (some (fn [[_ leases]]
                 (some (fn [[_ lease]]
                         (and (= client-id (:client-id lease))
                              (= room-id (:room-id lease))
                              (slots/active-lease? lease)))
                       leases))
               (:slots state))))))

(defn joined-room!
  [state* room]
  (swap! state* assoc-in [:joined-rooms (:roomId room)] room)
  room)

(defn joined-room
  [state room-id]
  (get-in state [:joined-rooms room-id]))

(defn slot-room
  [state project-id slot]
  (get-in state [:slot-rooms project-id slot]))

(defn remember-slot-room!
  [state* project-id slot room]
  (let [room (assoc room
                    :project-id project-id
                    :slot slot)]
    (swap! state* assoc-in [:slot-rooms project-id slot] room)
    room))

(defn acquire-slot!
  [state* {:keys [now]} {:keys [client-id project room-id room-name]}]
  (let [now (or now (now-ms))
        project-id (:id project)]
    (when-not project-id
      (throw (ex-info "Project id is required to acquire a slot." {:code :invalid_request})))
    (let [result* (atom nil)]
      (swap! state*
             (fn [state]
               (require-client state client-id)
               (let [leases (get-in state [:slots project-id] {})
                     active (into {} (filter (comp slots/active-lease? val) leases))
                     slot (slots/first-free-slot active)
                     lease {:slot slot
                            :project-id project-id
                            :room-id room-id
                            :room-name room-name
                            :client-id client-id
                            :client-metadata (get-in state [:clients client-id :metadata])
                            :state :leased
                            :acquired-at now
                            :last-heartbeat-at now}]
                 (reset! result* lease)
                 (-> state
                     (assoc-in [:slots project-id slot] lease)
                     (update-in [:clients client-id :acquired-rooms] (fnil conj #{}) room-id)))))
      @result*)))

(defn list-slots
  [state project-id]
  {:projectId project-id
   :slots (->> (get-in state [:slots project-id] {})
               vals
               (sort-by slots/lease-sort-key)
               (mapv (fn [lease]
                       {:slot (:slot lease)
                        :roomId (:room-id lease)
                        :roomName (:room-name lease)
                        :clientId (:client-id lease)
                        :clientMetadata (:client-metadata lease)
                        :state (name (:state lease))
                        :acquiredAt (:acquired-at lease)
                        :lastHeartbeatAt (:last-heartbeat-at lease)})))})

(defn release-slot!
  [state* {:keys [client-id room-id slot]}]
  (let [released? (atom false)]
    (swap! state*
           (fn [state]
             (require-client state client-id)
             (reduce-kv
              (fn [state project-id leases]
                (if-let [[slot-key lease]
                         (some (fn [[slot-key lease]]
                                 (when (and (or (nil? slot) (= slot slot-key))
                                            (or (nil? room-id) (= room-id (:room-id lease)))
                                            (= client-id (:client-id lease))
                                            (slots/active-lease? lease))
                                   [slot-key lease]))
                               leases)]
                  (do
                    (reset! released? true)
                    (-> state
                        (assoc-in [:slots project-id slot-key :state] :released)
                        (update-in [:clients client-id :acquired-rooms] disj (:room-id lease))))
                  state))
              state
              (:slots state))))
    {:released @released?}))

(defn mark-client-suspect!
  [state* client-id now]
  (swap! state*
         (fn [state]
           (require-client state client-id)
           (update state :slots
                   (fn [slots-by-project]
                     (into {}
                           (map (fn [[project-id leases]]
                                  [project-id
                                   (into {}
                                         (map (fn [[slot lease]]
                                                [slot (cond-> lease
                                                        (and (= client-id (:client-id lease))
                                                             (slots/active-lease? lease))
                                                        (assoc :state :suspect
                                                               :suspect-at now))]))
                                         leases)]))
                           slots-by-project))))))

(defn append-event!
  [state* event]
  (let [result* (atom nil)]
    (swap! state*
           (fn [{:keys [next-event-id] :as state}]
             (let [event-id (str "evt-" next-event-id)
                   event (assoc event :id event-id)]
               (reset! result* event)
               (-> state
                   (update :next-event-id inc)
                   (update :events conj event)))))
    @result*))

(defn replay-events-after
  [state last-event-id client-id]
  (let [events (:events state)
        start-idx (if last-event-id
                    (inc (or (first (keep-indexed (fn [idx ev]
                                                    (when (= last-event-id (:id ev)) idx))
                                                  events))
                             (dec (count events))))
                    (count events))
        client (require-client state client-id)
        rooms (:subscriptions client)]
    (->> events
         (drop start-idx)
         (filter (fn [ev]
                   (if-let [room-id (get-in ev [:data :room :roomId])]
                     (contains? rooms room-id)
                     true)))
         vec)))

(defn mark-stale-leases!
  [state* now heartbeat-seconds stale-after-missed]
  (let [cutoff (- now (* heartbeat-seconds stale-after-missed 1000))
        stale* (atom [])]
    (swap! state*
           (fn [state]
             (let [state (update state :slots
                                 (fn [slots-by-project]
                                   (into {}
                                         (map (fn [[project-id leases]]
                                                [project-id
                                                 (into {}
                                                       (map (fn [[slot lease]]
                                                              (if (and (slots/active-lease? lease)
                                                                       (< (:last-heartbeat-at lease) cutoff))
                                                                (do
                                                                  (swap! stale* conj lease)
                                                                  [slot (assoc lease :state :released
                                                                               :released-at now
                                                                               :release-reason :stale)])
                                                                [slot lease])))
                                                       leases)]))
                                         slots-by-project)))]
               (reduce (fn [state {:keys [client-id room-id]}]
                         (update-in state [:clients client-id :acquired-rooms] disj room-id))
                       state
                       @stale*))))
    @stale*))