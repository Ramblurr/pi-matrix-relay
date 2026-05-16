(ns pi-matrix-relay.broker-client
  (:require [clojure.string :as str]
            [pi-matrix-relay.config :as config]))

(def http (js/require "http"))
(def path (js/require "path"))

(defn socket-path
  "Return the broker Unix domain socket path for a Node `process.env` object."
  ([]
   (socket-path (.-env js/process)))
  ([env]
   (.join path (or (aget (or env #js {}) "XDG_RUNTIME_DIR")
                   "/tmp")
          config/app-name
          "broker.sock")))

(defn request-options
  [env method uri body]
  (let [headers (cond-> {"Accept" "application/json"}
                  body (assoc "Content-Type" "application/json"
                              "Content-Length" (.-length (js/Buffer.from body "utf8"))))]
    (clj->js {:socketPath (socket-path env)
              :path uri
              :method method
              :headers headers})))

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

(defn- parse-json
  [text]
  (when-not (str/blank? text)
    (js->clj (js/JSON.parse text) :keywordize-keys true)))

(defn request-json!
  ([method uri body]
   (request-json! {:env (.-env js/process)} method uri body))
  ([{:keys [env]} method uri body]
   (let [payload (when body (js/JSON.stringify (clj->js body)))]
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
                                         (resolve (unwrap-envelope (parse-json (apply str @chunks))))
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
   (request-json! opts "GET" "/v1/health" nil)))

(defn resolve-room!
  ([room]
   (resolve-room! {} room))
  ([opts room]
   (request-json! opts "POST" "/v1/matrix/rooms/resolve" {:room room})))

(defn send-message!
  ([room-id message]
   (send-message! {} room-id message))
  ([opts room-id message]
   (request-json! opts "POST" "/v1/matrix/messages"
                  {:target {:roomId room-id}
                   :body message})))
