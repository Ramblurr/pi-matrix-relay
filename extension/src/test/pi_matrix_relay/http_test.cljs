(ns pi-matrix-relay.http-test
  (:require [cljs.test :refer [deftest is]]
            [pi-matrix-relay.http :as http]))

(deftest socket-path-prefers-xdg-runtime-dir
  (is (= "/run/user/1000/pi-matrix-relay/broker.sock"
         (http/socket-path #js {"XDG_RUNTIME_DIR" "/run/user/1000"})))
  (is (= "/tmp/pi-matrix-relay/broker.sock"
         (http/socket-path #js {}))))

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
