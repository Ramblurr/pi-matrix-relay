(ns pi-matrix-relay.broker-client
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [ol.dirs :as dirs]
            [pi-matrix-relay.config :as config]))

(def http (js/require "http"))
(def path (js/require "path"))

(defn socket-path
  "Return the broker Unix domain socket path."
  ([]
   (or (dirs/runtime-dir config/app-name)
       (.join path "/tmp" config/app-name "broker.sock")))
  ([env]
   (.join path (or (aget (or env #js {}) "XDG_RUNTIME_DIR")
                   "/tmp")
          config/app-name
          "broker.sock")))

(defn request-options
  [env method uri body]
  (let [headers (cond-> {"Accept" "application/edn"}
                  body (assoc "Content-Type" "application/edn"
                              "Content-Length" (.-length (js/Buffer.from body "utf8"))))]
    (clj->js {:socketPath (socket-path env)
              :path uri
              :method method
              :headers headers})))

(defn encode-path-segment
  [segment]
  (js/encodeURIComponent (str segment)))

(defn- client-path
  ([client-id]
   (client-path client-id nil))
  ([client-id suffix]
   (str "/v1/clients/" (encode-path-segment client-id) suffix)))

(defn event-stream-options
  [env client-id]
  (clj->js {:socketPath (socket-path env)
            :path (client-path client-id "/events")
            :method "GET"
            :headers {"Accept" "text/event-stream"}}))

(defn broker-error
  [{:keys [message code details]}]
  (let [err (js/Error. (or message "Broker request failed"))]
    (set! (.-data err) (clj->js {:code code
                                 :details (or details {})}))
    err))

(defn unwrap-envelope
  [envelope]
  (if (:ok envelope)
    (:data envelope)
    (throw (broker-error (:error envelope)))))

(defn- parse-edn
  [text]
  (when-not (str/blank? text)
    (reader/read-string text)))

(defn- parse-sse-frame
  [frame]
  (let [lines (str/split-lines frame)
        parsed (reduce (fn [acc line]
                         (cond
                           (str/starts-with? line ":")
                           acc

                           (str/starts-with? line "id:")
                           (assoc acc :id (str/trim (subs line 3)))

                           (str/starts-with? line "event:")
                           (assoc acc :event (str/trim (subs line 6)))

                           (str/starts-with? line "data:")
                           (update acc :data-lines (fnil conj []) (str/triml (subs line 5)))

                           :else
                           acc))
                       {}
                       lines)]
    (when (seq (:data-lines parsed))
      {:id (:id parsed)
       :event (:event parsed)
       :data (parse-edn (str/join "\n" (:data-lines parsed)))})))

(defn parse-sse-chunk
  [buffer chunk]
  (let [combined (str (or buffer "") chunk)
        parts (str/split combined #"\n\n" -1)
        frames (butlast parts)]
    {:events (->> frames
                  (keep parse-sse-frame)
                  vec)
     :buffer (last parts)}))

(defn request-edn!
  ([method uri body]
   (request-edn! {:env (.-env js/process)} method uri body))
  ([{:keys [env]} method uri body]
   (let [env (or env (.-env js/process))
         payload (when (some? body) (pr-str body))]
     (js/Promise.
      (fn [resolve reject]
        (let [req (.request http
                            (request-options env method uri payload)
                            (fn [^js res]
                              (let [chunks (atom [])]
                                (.setEncoding res "utf8")
                                (.on res "data" #(swap! chunks conj %))
                                (.on res "end"
                                     (fn []
                                       (try
                                         (resolve (unwrap-envelope (parse-edn (apply str @chunks))))
                                         (catch js/Error err
                                           (reject err))))))))]
          (.on req "error" reject)
          (when payload
            (.write req payload))
          (.end req)))))))

(defn health!
  ([]
   (health! {}))
  ([opts]
   (request-edn! opts "GET" "/v1/health" nil)))

(defn register-client!
  ([request]
   (register-client! {} request))
  ([opts request]
   (request-edn! opts "POST" "/v1/clients" request)))

(defn update-subscriptions!
  ([client-id rooms]
   (update-subscriptions! {} client-id rooms))
  ([opts client-id rooms]
   (request-edn! opts "PATCH" (client-path client-id "/subscriptions")
                  {:rooms rooms})))

(defn- client-room-path
  [client-id room-id suffix]
  (str (client-path client-id "/rooms/")
       (encode-path-segment room-id)
       suffix))

(defn get-room-delivery-mode!
  ([client-id room-id]
   (get-room-delivery-mode! {} client-id room-id))
  ([opts client-id room-id]
   (request-edn! opts "GET" (client-room-path client-id room-id "/delivery-mode") nil)))

(defn set-room-delivery-mode!
  ([client-id room-id default-delivery-mode updated-by-user]
   (set-room-delivery-mode! {} client-id room-id default-delivery-mode updated-by-user))
  ([opts client-id room-id default-delivery-mode updated-by-user]
   (request-edn! opts "PUT" (client-room-path client-id room-id "/delivery-mode")
                  {:room/default-delivery-mode default-delivery-mode
                   :room/default-delivery-mode-updated-by-user updated-by-user})))

(defn heartbeat!
  ([client-id]
   (heartbeat! {} client-id))
  ([opts client-id]
   (request-edn! opts "POST" (client-path client-id "/heartbeat") {})))

(defn unregister-client!
  ([client-id reason]
   (unregister-client! {} client-id reason))
  ([opts client-id reason]
   (request-edn! opts "DELETE" (client-path client-id) {:reason reason})))

(defn acquire-slot!
  ([client-id project invite]
   (acquire-slot! {} client-id project invite))
  ([opts client-id project invite]
   (request-edn! opts "POST" "/v1/slots/acquire"
                  (cond-> {:client/id client-id
                           :project project}
                    (seq invite) (assoc :invite invite)))))

(defn release-slot!
  ([client-id room-id slot]
   (release-slot! {} client-id room-id slot))
  ([opts client-id room-id slot]
   (request-edn! opts "POST" "/v1/slots/release"
                  {:client/id client-id
                   :room/id room-id
                   :slot slot})))

(defn list-slots!
  ([project-id]
   (list-slots! {} project-id))
  ([opts project-id]
   (request-edn! opts "GET" (str "/v1/slots?project-id=" (encode-path-segment project-id)) nil)))

(defn list-rooms!
  ([]
   (list-rooms! {}))
  ([opts]
   (request-edn! opts "GET" "/v1/matrix/rooms" nil)))

(defn- now-ms []
  (.getTime (js/Date.)))

(defn- error-data
  [err]
  (cond-> {:message (.-message err)}
    (.-code err) (assoc :code (.-code err))))

(defn open-event-stream!
  ([client-id on-event]
   (open-event-stream! {:env (.-env js/process)} client-id on-event))
  ([{:keys [env on-error]} client-id on-event]
   (let [env (or env (.-env js/process))
         buffer* (atom "")
         state* (atom {:client/id client-id
                       :stream/started-at (now-ms)
                       :stream/connected? false
                       :stream/closed? false
                       :chunk/count 0
                       :event/count 0})
         record-error! (fn [err]
                         (swap! state* assoc
                                :error/last (error-data err)
                                :error/last-at (now-ms))
                         (when on-error
                           (on-error err)))
         req (.request http
                       (event-stream-options env client-id)
                       (fn [^js res]
                         (let [status-code (.-statusCode res)]
                           (swap! state* assoc
                                  :http/status-code status-code
                                  :response/at (now-ms)
                                  :stream/connected? (= 200 status-code))
                           (when (not= 200 status-code)
                             (record-error! (js/Error. (str "Event stream HTTP " status-code))))
                           (.setEncoding res "utf8")
                           (.on res "data"
                                (fn [chunk]
                                  (swap! state* #(-> %
                                                     (update :chunk/count (fnil inc 0))
                                                     (assoc :chunk/last-at (now-ms))))
                                  (try
                                    (let [{:keys [events buffer]} (parse-sse-chunk @buffer* chunk)]
                                      (reset! buffer* buffer)
                                      (doseq [event events]
                                        (swap! state* #(-> %
                                                           (update :event/count (fnil inc 0))
                                                           (assoc :event/last-at (now-ms)
                                                                  :event/last-id (:id event)
                                                                  :event/last-type (:event event))))
                                        (on-event (:data event))))
                                    (catch js/Error err
                                      (record-error! err)))))
                           (.on res "end"
                                (fn []
                                  (swap! state* assoc
                                         :stream/closed? true
                                         :stream/closed-at (now-ms)
                                         :stream/close-reason "response-end")))
                           (.on res "close"
                                (fn []
                                  (swap! state* assoc
                                         :stream/closed? true
                                         :stream/closed-at (now-ms)
                                         :stream/close-reason "response-close"))))))]
     (.on req "error" record-error!)
     (.on req "close" (fn []
                         (swap! state* assoc :request/closed-at (now-ms))))
     (.end req)
     #js {:close (fn []
                   (swap! state* assoc
                          :close/requested? true
                          :close/requested-at (now-ms))
                   (.destroy req))
          :diagnostics (fn []
                         (clj->js @state*))})))

(defn resolve-room!
  ([room]
   (resolve-room! {} room))
  ([opts room]
   (request-edn! opts "POST" "/v1/matrix/rooms/resolve" {:room room})))

(defn send-message!
  ([room-id message]
   (send-message! {} room-id message nil))
  ([opts-or-room-id room-id-or-message message-or-opts]
   (if (map? opts-or-room-id)
     (send-message! opts-or-room-id room-id-or-message message-or-opts nil)
     (send-message! {} opts-or-room-id room-id-or-message message-or-opts)))
  ([opts room-id message send-opts]
   (request-edn! opts "POST" "/v1/matrix/messages"
                  (cond-> {:target {:room/id room-id}
                           :body message}
                    (:client/id send-opts)
                    (assoc :client/id (:client/id send-opts))

                    (:reply-to/event-id send-opts)
                    (assoc :reply-to {:room/id room-id
                                      :event/id (:reply-to/event-id send-opts)})))))

(defn send-reaction!
  ([room-id event-id key]
   (send-reaction! {} room-id event-id key nil))
  ([opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts]
   (if (map? opts-or-room-id)
     (send-reaction! opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts nil)
     (send-reaction! {} opts-or-room-id room-id-or-event-id event-id-or-key key-or-opts)))
  ([opts room-id event-id key send-opts]
   (request-edn! opts "POST" "/v1/matrix/reactions"
                  (cond-> {:room/id room-id
                           :event/id event-id
                           :key key}
                    (:client/id send-opts)
                    (assoc :client/id (:client/id send-opts))))))
