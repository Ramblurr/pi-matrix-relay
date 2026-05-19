(ns pi-matrix-relay.broker.repl
  "REPL helpers for live broker Matrix verification work.

  These functions are intentionally small wrappers around broker and public
  `trixnity-clj` APIs. They return plain EDN maps where possible so a Pi agent
  can use them from `brepl` without writing nested Missionary/Trixnity plumbing.

  ## Element verification workflow

  Element verification against the bot creates an encrypted direct-message room
  request. The broker should auto-join direct-message invites, but when debugging
  a running session you can still force activation of recent request events:

  ```clojure
  (require '[pi-matrix-relay.broker.repl :as r] :reload)

  (r/activate-recent-room-requests! {:limit 5 :room-timeout-ms 10000})
  (r/verification-summaries)

  ;; Accept the incoming verification request.
  (r/accept-current!)

  ;; After the operator clicks “verify by emoji” in Element, accept the SAS start.
  (r/accept-current!)

  ;; Read emoji in the terminal and compare with Element.
  (r/current-verification-summary)

  ;; If the operator says they match:
  (r/confirm-current!)

  ;; If they do not match:
  (r/no-match-current!)
  ```

  During live Element verification, keep instructions and emoji in the Pi
  terminal rather than Matrix messages. Switching away from Element to read the
  Matrix relay room can reset or time out the verification UI."
  (:require
   [missionary.core :as m]
   [ol.trixnity.event :as event]
   [ol.trixnity.room :as room]
   [ol.trixnity.schemas :as mx]
   [ol.trixnity.verification :as verification]
   [pi-matrix-relay.broker.matrix :as matrix]))

(set! *warn-on-reflection* true)

(defn- unqualify-key
  [k]
  (if (qualified-keyword? k)
    (keyword (name k))
    k))

(defn- plain-edn
  [value]
  (cond
    (map? value)
    (into {}
          (map (fn [[k v]] [(unqualify-key k) (plain-edn v)]))
          value)

    (sequential? value)
    (mapv plain-edn value)

    (set? value)
    (set (map plain-edn value))

    :else value))

(defn dev-gateway
  "Returns the running REPL-managed Matrix gateway from the `dev` namespace."
  []
  (let [instance (or (requiring-resolve 'dev/instance)
                     (throw (ex-info "The dev namespace is not loaded. Start the broker with `cd broker && bb dev`."
                                     {})))
        gateway (instance [:broker :matrix-gateway])]
    (or gateway
        (throw (ex-info "No running Matrix gateway found. Start or reset the broker from the `dev` namespace."
                        {:component-id [:broker :matrix-gateway]})))))

(defn matrix-client
  "Returns the live Trixnity client stored in `gateway`."
  ([]
   (matrix-client (dev-gateway)))
  ([gateway]
   (let [runtime* (:runtime* gateway)
         client (:client @runtime*)]
     (or client
         (throw (ex-info "No live Matrix client found in the gateway runtime."
                         {:has-runtime? (boolean runtime*)}))))))

(defn verification-status
  "Returns current Matrix verification status from `gateway`."
  ([]
   (verification-status (dev-gateway)))
  ([gateway]
   (matrix/verification-status gateway)))

(declare accept! confirm! no-match! cancel!)

(defn- sas-emoji-summary
  [emoji]
  (let [emoji (plain-edn emoji)]
    (cond-> {}
      (:emoji emoji) (assoc :emoji (:emoji emoji))
      (:description emoji) (assoc :description (:description emoji)))))

(defn verification-summary
  "Returns a concise summary of one active Matrix verification map."
  [verification]
  (when verification
    (let [verification (plain-edn verification)
          state (:verification-state verification)
          sas-state (:sas-state state)]
      (cond-> {}
        (:verification-id verification) (assoc :id (:verification-id verification))
        (:verification-kind verification) (assoc :kind (:verification-kind verification))
        (:verification-direction verification) (assoc :direction (:verification-direction verification))
        (:their-user-id verification) (assoc :their-user-id (:their-user-id verification))
        (:their-device-id verification) (assoc :their-device-id (:their-device-id verification))
        (:room-id verification) (assoc :room-id (:room-id verification))
        (:request-event-id verification) (assoc :request-event-id (:request-event-id verification))
        (:transaction-id verification) (assoc :transaction-id (:transaction-id verification))
        (:kind state) (assoc :state (:kind state))
        (:kind sas-state) (assoc :sas-state (:kind sas-state))
        (:sas-emojis sas-state) (assoc :emojis (mapv sas-emoji-summary (:sas-emojis sas-state)))
        (:sas-decimal sas-state) (assoc :decimal (:sas-decimal sas-state))
        (:reason state) (assoc :reason (:reason state))
        (:cancel-code state) (assoc :cancel-code (:cancel-code state))))))

(defn verification-summaries
  "Returns concise summaries for all active Matrix verifications."
  ([]
   (verification-summaries (dev-gateway)))
  ([gateway]
   (mapv verification-summary (:verifications (verification-status gateway)))))

(defn current-verification-summary
  "Returns the first active Matrix verification summary, or nil."
  ([]
   (current-verification-summary (dev-gateway)))
  ([gateway]
   (first (verification-summaries gateway))))

(defn- current-verification-id
  [gateway]
  (or (:id (current-verification-summary gateway))
      (throw (ex-info "No active Matrix verification found." {}))))

(defn accept-current!
  "Accepts the current verification request or pending SAS start.

  This intentionally calls [[accept!]] for both the initial incoming request and
  the later `their-sas-start` state used after Element clicks “verify by emoji”."
  ([]
   (accept-current! (dev-gateway)))
  ([gateway]
   (verification-summary (accept! gateway (current-verification-id gateway)))))

(defn confirm-current!
  "Confirms the current verification after matching SAS emoji with Element."
  ([]
   (confirm-current! (dev-gateway)))
  ([gateway]
   (verification-summary (confirm! gateway (current-verification-id gateway)))))

(defn no-match-current!
  "Rejects the current verification after SAS emoji do not match Element."
  ([]
   (no-match-current! (dev-gateway)))
  ([gateway]
   (verification-summary (no-match! gateway (current-verification-id gateway)))))

(defn cancel-current!
  "Cancels the current active verification."
  ([]
   (cancel-current! (dev-gateway)))
  ([gateway]
   (verification-summary (cancel! gateway (current-verification-id gateway)))))

(defn start-user-verification!
  "Starts Matrix user verification for `user-id`."
  ([user-id]
   (start-user-verification! (dev-gateway) user-id))
  ([gateway user-id]
   (matrix/verification-start! gateway {:user/id user-id})))

(defn start-device-verification!
  "Starts Matrix device verification for `user-id` and `device-id`."
  ([user-id device-id]
   (start-device-verification! (dev-gateway) user-id device-id))
  ([gateway user-id device-id]
   (matrix/verification-start! gateway {:user/id user-id
                                        :device/id device-id})))

(defn accept!
  "Accepts the active verification identified by `verification-id`."
  ([verification-id]
   (accept! (dev-gateway) verification-id))
  ([gateway verification-id]
   (matrix/verification-accept! gateway verification-id)))

(defn start-sas!
  "Starts SAS for the active verification identified by `verification-id`."
  ([verification-id]
   (start-sas! (dev-gateway) verification-id))
  ([gateway verification-id]
   (matrix/verification-start-sas! gateway verification-id)))

(defn confirm!
  "Confirms matching SAS emoji for `verification-id`."
  ([verification-id]
   (confirm! (dev-gateway) verification-id))
  ([gateway verification-id]
   (matrix/verification-confirm! gateway verification-id)))

(defn no-match!
  "Rejects SAS emoji for `verification-id` as non-matching."
  ([verification-id]
   (no-match! (dev-gateway) verification-id))
  ([gateway verification-id]
   (matrix/verification-no-match! gateway verification-id)))

(defn cancel!
  "Cancels the active verification identified by `verification-id`."
  ([verification-id]
   (cancel! (dev-gateway) verification-id))
  ([gateway verification-id]
   (matrix/verification-cancel! gateway verification-id)))

(defn- take-flow
  [n flow]
  (m/? (->> flow
            (m/eduction (take n))
            (m/reduce conj []))))

(defn- first-flow
  [flow]
  (first (take-flow 1 flow)))

(defn- with-timeout
  [timeout-ms f]
  (if timeout-ms
    (let [timeout-sentinel (Object.)
          task (future (f))
          result (deref task timeout-ms timeout-sentinel)]
      (if (identical? result timeout-sentinel)
        (do
          (future-cancel task)
          (throw (ex-info "Timed out while reading Matrix timeline events."
                          {:timeout-ms timeout-ms})))
        result))
    (f)))

(defn summarize-event
  "Returns a compact EDN summary for normalized Matrix timeline `ev`."
  [ev]
  (let [candidate? (and (event/text? ev)
                        (nil? (event/body ev)))]
    (cond-> {:room/id (event/room-id ev)
             :event/id (event/event-id ev)
             :event/type (event/type ev)
             :event/sender (event/sender ev)
             :event/body (event/body ev)
             :message/type (event/msgtype ev)
             :verification/request-candidate? candidate?}
      (some? (get ev ::mx/content))
      (assoc :content/class (str (class (get ev ::mx/content)))))))

(defn recent-events
  "Returns recent Matrix timeline events for `room-id`.

  Options:

  | key           | description
  |---------------|------------
  | `:limit`      | Maximum events to return (default `20`)
  | `:timeout-ms` | Maximum time to wait for this room scan (default `5000`)"
  ([room-id]
   (recent-events (matrix-client) room-id {}))
  ([room-id opts]
   (recent-events (matrix-client) room-id opts))
  ([client room-id opts]
   (let [limit (or (:limit opts) 20)
         timeout-ms (or (:timeout-ms opts) 5000)]
     (with-timeout
      timeout-ms
      (fn []
        (let [latest-flow (first-flow (room/get-last-timeline-event client room-id))
              latest-event (when latest-flow (first-flow latest-flow))
              event-flows (if latest-event
                            (take-flow limit
                                       (room/get-timeline-events
                                        client
                                        room-id
                                        (event/event-id latest-event)
                                        :backwards
                                        {::mx/max-size limit
                                         ::mx/min-size 1}))
                            [])]
          (mapv summarize-event
                (keep (fn [event-flow]
                        (try
                          (first-flow event-flow)
                          (catch Throwable _
                            nil)))
                      event-flows))))))))

(defn verification-request-candidates
  "Finds recent room-message events that may be Matrix room verification requests."
  ([]
   (verification-request-candidates (dev-gateway) {}))
  ([opts]
   (verification-request-candidates (dev-gateway) opts))
  ([gateway opts]
   (let [client (matrix-client gateway)
         limit (or (:limit opts) 20)
         timeout-ms (or (:room-timeout-ms opts) 5000)
         room-ids (or (:room-ids opts)
                      (mapv :room/id (matrix/list-rooms! gateway)))]
     (->> room-ids
          (mapcat (fn [room-id]
                    (try
                      (recent-events client room-id {:limit limit
                                                     :timeout-ms timeout-ms})
                      (catch Throwable ex
                        [{:room/id room-id
                          :error (ex-message ex)}]))))
          (filter :verification/request-candidate?)
          vec))))

(defn activate-room-request!
  "Activates a room-scoped Matrix verification request by `room-id` and `event-id`."
  ([room-id event-id]
   (activate-room-request! (matrix-client) room-id event-id))
  ([client room-id event-id]
   (plain-edn
    (m/? (verification/get-active-user-verification! client room-id event-id)))))

(defn activate-recent-room-requests!
  "Finds and activates recent room-scoped Matrix verification request candidates."
  ([]
   (activate-recent-room-requests! (dev-gateway) {}))
  ([opts]
   (activate-recent-room-requests! (dev-gateway) opts))
  ([gateway opts]
   (let [client (matrix-client gateway)]
     (mapv (fn [candidate]
             (assoc candidate
                    :activation/result
                    (try
                      (activate-room-request! client (:room/id candidate) (:event/id candidate))
                      (catch Throwable ex
                        {:error (ex-message ex)}))))
           (verification-request-candidates gateway opts)))))
