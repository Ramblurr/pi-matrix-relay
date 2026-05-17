(ns pi-matrix-relay.broker.runtime)

(def default-event-buffer-size 1000)

(defn create-runtime
  ([]
   (create-runtime {}))
  ([{:keys [event-buffer-size]}]
   {:subscribers* (atom {})
    :verifications* (atom {})
    ;; Process-local replay buffer. Phase 2 replaces this with durable events.
    :events* (atom {:next-event-id 0
                    :limit (or event-buffer-size default-event-buffer-size)
                    :events []})}))

(defn subscribe!
  [runtime client-id channel]
  (swap! (:subscribers* runtime) update client-id (fnil conj #{}) channel)
  nil)

(defn unsubscribe!
  [runtime client-id channel]
  (swap! (:subscribers* runtime) update client-id disj channel)
  nil)

(defn subscriber-channels
  [runtime client-id]
  (get @(:subscribers* runtime) client-id))

(defn append-event!
  [runtime event]
  (let [[_old new] (swap-vals!
                    (:events* runtime)
                    (fn [{:keys [next-event-id limit events] :as state}]
                      (let [event-id (str "evt-" next-event-id)
                            event (assoc event :id event-id)
                            events (conj events event)]
                        (assoc state
                               :next-event-id (inc next-event-id)
                               :events (if (> (count events) limit)
                                         (vec (take-last limit events))
                                         events)))))]
    (peek (:events new))))

(defn replay-events-after
  [runtime last-event-id]
  (let [events (:events @(:events* runtime))
        start-idx (if last-event-id
                    (inc (or (first (keep-indexed (fn [idx ev]
                                                    (when (= last-event-id (:id ev)) idx))
                                                  events))
                             (dec (count events))))
                    (count events))]
    (vec (drop start-idx events))))
