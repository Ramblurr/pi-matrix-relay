(ns pi-matrix-relay.http
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [ol.dirs :as dirs]
            [pi-matrix-relay.config :as config]))

(def node-http (js/require "http"))
(def path (js/require "path"))

(defn socket-path
  "Return the broker Unix domain socket path."
  ([]
   (if-let [runtime-dir (dirs/runtime-dir config/app-name)]
     (.join path runtime-dir "broker.sock")
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

(defn event-stream-options
  [env uri]
  (clj->js {:socketPath (socket-path env)
            :path uri
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
        (let [settled? (atom false)
              response-ended? (atom false)
              resolve-once! (fn [value]
                              (when (compare-and-set! settled? false true)
                                (resolve value)))
              reject-once! (fn [err]
                             (when (compare-and-set! settled? false true)
                               (reject err)))
              req (.request node-http
                            (request-options env method uri payload)
                            (fn [^js res]
                              (let [chunks #js []]
                                (.setEncoding res "utf8")
                                (.on res "data" #(.push chunks %))
                                (.on res "error" reject-once!)
                                (.on res "aborted"
                                     (fn []
                                       (reject-once! (js/Error. "Broker response aborted"))))
                                (.on res "close"
                                     (fn []
                                       (when-not @response-ended?
                                         (reject-once! (js/Error. "Broker response closed before end")))))
                                (.on res "end"
                                     (fn []
                                       (reset! response-ended? true)
                                       (try
                                         (resolve-once! (unwrap-envelope (parse-edn (.join chunks ""))))
                                         (catch js/Error err
                                           (reject-once! err))))))))]
          (.on req "error" reject-once!)
          (when payload
            (.write req payload))
          (.end req)))))))

(defn- now-ms []
  (.getTime (js/Date.)))

(defn- error-data
  [err]
  (cond-> {:message (.-message err)}
    (.-code err) (assoc :code (.-code err))))

(defn- event-stream-diagnostics
  [state]
  (dissoc state :sse/buffer :close/notified?))

(defn- diagnostic-key
  [k]
  (if (keyword? k)
    (subs (str k) 1)
    (str k)))

(defn- diagnostics->js
  [state]
  (clj->js (into {}
                 (map (fn [[k v]] [(diagnostic-key k) v]))
                 (event-stream-diagnostics state))))

(defn- parse-sse-chunk!
  [state* chunk]
  (let [{:keys [events buffer]} (parse-sse-chunk (:sse/buffer @state*) chunk)]
    (swap! state* #(-> %
                       (update :chunk/count (fnil inc 0))
                       (assoc :chunk/last-at (now-ms)
                              :sse/buffer buffer)))
    events))

(defn- record-sse-event!
  [state* event]
  (swap! state* #(-> %
                     (update :event/count (fnil inc 0))
                     (assoc :event/last-at (now-ms)
                            :event/last-id (:id event)
                            :event/last-type (:event event)))))

(defn open-event-stream!
  ([uri on-event]
   (open-event-stream! {:env (.-env js/process)} uri on-event))
  ([{:keys [env on-error on-close diagnostics]} uri on-event]
   (let [env (or env (.-env js/process))
         state* (atom (merge {:stream/path uri
                              :stream/started-at (now-ms)
                              :stream/connected? false
                              :stream/closed? false
                              :chunk/count 0
                              :event/count 0
                              :sse/buffer ""
                              :close/notified? false}
                             diagnostics))
         notify-close! (fn [reason]
                         (when-not (:close/notified? @state*)
                           (let [state (swap! state* assoc
                                              :stream/closed? true
                                              :stream/closed-at (now-ms)
                                              :stream/close-reason reason
                                              :close/notified? true)]
                             (when on-close
                               (on-close (event-stream-diagnostics state))))))
         record-error! (fn [err]
                         (swap! state* assoc
                                :error/last (error-data err)
                                :error/last-at (now-ms))
                         (when on-error
                           (on-error err)))
         req (.request node-http
                       (event-stream-options env uri)
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
                                  (try
                                    (doseq [event (parse-sse-chunk! state* chunk)]
                                      (record-sse-event! state* event)
                                      (on-event (:data event)))
                                    (catch js/Error err
                                      (record-error! err)))))
                           (.on res "end"
                                (fn []
                                  (notify-close! "response-end")))
                           (.on res "close"
                                (fn []
                                  (notify-close! "response-close"))))))]
     (.on req "error" (fn [err]
                         (record-error! err)
                         (notify-close! "request-error")))
     (.on req "close" (fn []
                         (swap! state* assoc :request/closed-at (now-ms))))
     (.end req)
     #js {:close (fn []
                   (swap! state* assoc
                          :close/requested? true
                          :close/requested-at (now-ms))
                   (.destroy req))
          :diagnostics (fn []
                         (diagnostics->js @state*))})))
