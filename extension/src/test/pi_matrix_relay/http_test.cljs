(ns pi-matrix-relay.http-test
  (:require [cljs.test :refer [async deftest is]]
            [ol.dirs :as dirs]
            [pi-matrix-relay.http :as http]))

(defn- direct-call-compatible-fn1
  [f]
  (set! (.-cljs$core$IFn$_invoke$arity$1 f) f)
  f)

(deftest socket-path-prefers-xdg-runtime-dir
  (is (= "/run/user/1000/pi-matrix-relay/broker.sock"
         (http/socket-path #js {"XDG_RUNTIME_DIR" "/run/user/1000"})))
  (is (= "/tmp/pi-matrix-relay/broker.sock"
         (http/socket-path #js {}))))

(deftest socket-path-zero-arity-appends-socket-file
  (with-redefs [dirs/runtime-dir (direct-call-compatible-fn1
                                  (fn [application]
                                    (str "/run/user/1000/" application)))]
    (is (= "/run/user/1000/pi-matrix-relay/broker.sock"
           (http/socket-path))))
  (with-redefs [dirs/runtime-dir (direct-call-compatible-fn1
                                  (fn [_application] nil))]
    (is (= "/tmp/pi-matrix-relay/broker.sock"
           (http/socket-path)))))

(deftest request-options-use-unix-domain-socket
  (is (= {:socketPath "/run/user/1000/pi-matrix-relay/broker.sock"
          :path "/v1/health"
          :method "GET"
          :headers {:Accept "application/edn"}}
         (js->clj (http/request-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                        "GET"
                                        "/v1/health"
                                        nil)
                  :keywordize-keys true)))
  (is (= {:Accept "application/edn"
          :Content-Type "application/edn"
          :Content-Length 15}
         (get (js->clj (http/request-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                              "POST"
                                              "/v1/matrix/messages"
                                              "{:room/id \"!r\"}")
                    :keywordize-keys true)
              :headers))))

(deftest unwrap-envelope-returns-data-or-throws-error
  (is (= {:status "ok"}
         (http/unwrap-envelope {:ok true :data {:status "ok"}})))
  (try
    (http/unwrap-envelope {:ok false
                           :error {:code "matrix_not_configured"
                                   :message "Matrix missing"
                                   :details {:field "password"}}})
    (is false "expected broker error")
    (catch js/Error e
      (is (= "Matrix missing" (.-message e)))
      (is (= {:code "matrix_not_configured"
              :details {:field "password"}}
             (js->clj (.-data e) :keywordize-keys true))))))

(defn- request-outcome
  [trigger!]
  (let [res-handlers* (atom {})
        req-handlers* (atom {})
        fake-http #js {:request (fn [_opts callback]
                                  (let [res #js {:setEncoding (fn [_encoding])
                                                 :on (fn [event handler]
                                                       (swap! res-handlers* assoc event handler))}]
                                    (callback res)
                                    #js {:on (fn [event handler]
                                               (swap! req-handlers* assoc event handler))
                                         :write (fn [_body])
                                         :end (fn [])}))}
        timeout (js/Promise. (fn [resolve _reject]
                               (js/setTimeout #(resolve {:type :timeout}) 20)))]
    (with-redefs [http/node-http fake-http]
      (let [request (-> (http/request-edn! {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}
                                           "GET"
                                           "/v1/health"
                                           nil)
                        (.then (fn [value]
                                 {:type :resolved
                                  :value value}))
                        (.catch (fn [err]
                                  {:type :rejected
                                   :message (.-message err)})))]
        (trigger! res-handlers* req-handlers*)
        (js/Promise.race (clj->js [request timeout]))))))

(deftest request-edn-rejects-response-failures
  (async done
    (-> (js/Promise.all
         (clj->js [(request-outcome
                    (fn [res-handlers* _req-handlers*]
                      (when-let [handler (get @res-handlers* "error")]
                        (handler (js/Error. "boom")))))
                   (request-outcome
                    (fn [res-handlers* _req-handlers*]
                      (when-let [handler (get @res-handlers* "aborted")]
                        (handler))))
                   (request-outcome
                    (fn [res-handlers* _req-handlers*]
                      (when-let [handler (get @res-handlers* "close")]
                        (handler))))]))
        (.then (fn [results]
                 (is (= [{:type :rejected
                          :message "boom"}
                         {:type :rejected
                          :message "Broker response aborted"}
                         {:type :rejected
                          :message "Broker response closed before end"}]
                        (js->clj results :keywordize-keys true)))
                 (done)))
        (.catch (fn [err]
                  (is false (.-stack err))
                  (done))))))

(deftest parse-sse-chunk-returns-complete-events-and-buffer
  (let [result (http/parse-sse-chunk
                ""
                (str ": connected\n\n"
                     "id: evt-1\n"
                     "event: matrix.message\n"
                     "data: {:type \"matrix.message\", :room/id \"!room:example.org\", :event/text \"hello\"}\n\n"
                     "id: evt-2\n"))]
    (is (= [{:id "evt-1"
             :event "matrix.message"
             :data {:type "matrix.message"
                    :room/id "!room:example.org"
                    :event/text "hello"}}]
           (:events result)))
    (is (= "id: evt-2\n" (:buffer result)))))

(deftest event-stream-close-notifies-caller-with-diagnostics-once
  (let [res-handlers* (atom {})
        req-handlers* (atom {})
        closed* (atom [])
        fake-http #js {:request (fn [_opts callback]
                                  (let [res #js {:statusCode 200
                                                 :setEncoding (fn [_encoding])
                                                 :on (fn [event handler]
                                                       (swap! res-handlers* assoc event handler))}]
                                    (callback res)
                                    #js {:on (fn [event handler]
                                               (swap! req-handlers* assoc event handler))
                                         :end (fn [])
                                         :destroy (fn [])}))}]
    (with-redefs [http/node-http fake-http]
      (http/open-event-stream! {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                :diagnostics {:client/id "client-1"}
                                :on-close #(swap! closed* conj %)}
                               "/v1/clients/client-1/events"
                               (fn [_event]))
      ((get @res-handlers* "end"))
      ((get @res-handlers* "close"))
      (is (= 1 (count @closed*)))
      (let [closed (first @closed*)]
        (is (= {:client/id "client-1"
                :stream/path "/v1/clients/client-1/events"
                :stream/connected? true
                :stream/closed? true
                :stream/close-reason "response-end"
                :http/status-code 200}
               (select-keys closed [:client/id
                                    :stream/path
                                    :stream/connected?
                                    :stream/closed?
                                    :stream/close-reason
                                    :http/status-code])))
        (is (not (contains? closed :sse/buffer)))
        (is (not (contains? closed :close/notified?)))))))

(deftest event-stream-notifies-caller-when-http-stream-opens
  (let [res-handlers* (atom {})
        req-handlers* (atom {})
        opened* (atom [])
        fake-http #js {:request (fn [_opts callback]
                                  (let [res #js {:statusCode 200
                                                 :setEncoding (fn [_encoding])
                                                 :on (fn [event handler]
                                                       (swap! res-handlers* assoc event handler))}]
                                    (callback res)
                                    #js {:on (fn [event handler]
                                               (swap! req-handlers* assoc event handler))
                                         :end (fn [])
                                         :destroy (fn [])}))}]
    (with-redefs [http/node-http fake-http]
      (http/open-event-stream! {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                :diagnostics {:client/id "client-1"}
                                :on-open #(swap! opened* conj %)}
                               "/v1/clients/client-1/events"
                               (fn [_event]))
      (is (= 1 (count @opened*)))
      (is (= {:client/id "client-1"
              :stream/path "/v1/clients/client-1/events"
              :stream/connected? true
              :stream/closed? false
              :http/status-code 200}
             (select-keys (first @opened*) [:client/id
                                            :stream/path
                                            :stream/connected?
                                            :stream/closed?
                                            :http/status-code]))))))

(deftest event-stream-parses-split-events-and-updates-diagnostics
  (let [res-handlers* (atom {})
        req-handlers* (atom {})
        events* (atom [])
        fake-http #js {:request (fn [_opts callback]
                                  (let [res #js {:statusCode 200
                                                 :setEncoding (fn [_encoding])
                                                 :on (fn [event handler]
                                                       (swap! res-handlers* assoc event handler))}]
                                    (callback res)
                                    #js {:on (fn [event handler]
                                               (swap! req-handlers* assoc event handler))
                                         :end (fn [])
                                         :destroy (fn [])}))}]
    (with-redefs [http/node-http fake-http]
      (let [stream (http/open-event-stream! {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                             :diagnostics {:client/id "client-1"}}
                                            "/v1/clients/client-1/events"
                                            #(swap! events* conj %))]
        ((get @res-handlers* "data") "id: evt-1\nevent: matrix.message\ndata: {:room/id \"!room:example.org\"")
        (is (= [] @events*))
        ((get @res-handlers* "data") ", :event/text \"hello\"}\n\n")
        (is (= [{:room/id "!room:example.org"
                 :event/text "hello"}]
               @events*))
        (is (= {:client/id "client-1"
                :stream/path "/v1/clients/client-1/events"
                :stream/connected? true
                :stream/closed? false
                :chunk/count 2
                :event/count 1
                :event/last-id "evt-1"
                :event/last-type "matrix.message"}
               (select-keys (js->clj ((.-diagnostics stream)) :keywordize-keys true)
                            [:client/id
                             :stream/path
                             :stream/connected?
                             :stream/closed?
                             :chunk/count
                             :event/count
                             :event/last-id
                             :event/last-type])))))))
