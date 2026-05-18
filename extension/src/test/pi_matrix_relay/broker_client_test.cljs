(ns pi-matrix-relay.broker-client-test
  (:require [cljs.test :refer [async deftest is]]
            [pi-matrix-relay.broker-client :as broker-client]))

(deftest socket-path-prefers-xdg-runtime-dir
  (is (= "/run/user/1000/pi-matrix-relay/broker.sock"
         (broker-client/socket-path #js {"XDG_RUNTIME_DIR" "/run/user/1000"})))
  (is (= "/tmp/pi-matrix-relay/broker.sock"
         (broker-client/socket-path #js {}))))

(deftest request-options-use-unix-domain-socket
  (is (= {:socketPath "/run/user/1000/pi-matrix-relay/broker.sock"
          :path "/v1/health"
          :method "GET"
          :headers {:Accept "application/edn"}}
         (js->clj (broker-client/request-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                                 "GET"
                                                 "/v1/health"
                                                 nil)
                  :keywordize-keys true)))
  (is (= {:Accept "application/edn"
          :Content-Type "application/edn"
          :Content-Length 15}
         (get (js->clj (broker-client/request-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                                       "POST"
                                                       "/v1/matrix/messages"
                                                       "{:room/id \"!r\"}")
                    :keywordize-keys true)
              :headers))))

(deftest send-message-includes-optional-formatted-body
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}]
      (with-redefs [broker-client/request-edn! (fn
                                                  ([method uri body]
                                                   (swap! calls* conj [{} method uri body])
                                                   (js/Promise.resolve {:event/id "$event:example.org"}))
                                                  ([opts method uri body]
                                                   (swap! calls* conj [opts method uri body])
                                                   (js/Promise.resolve {:event/id "$event:example.org"})))]
        (-> (broker-client/send-message! opts
                                          "!room:example.org"
                                          "plain fallback"
                                          {:client/id "client-1"
                                           :reply-to/event-id "$parent:example.org"
                                           :formatted-body "<strong>formatted</strong>"})
            (.then (fn [_]
                     (is (= [[opts "POST" "/v1/matrix/messages"
                              {:target {:room/id "!room:example.org"}
                               :body "plain fallback"
                               :client/id "client-1"
                               :reply-to {:room/id "!room:example.org"
                                          :event/id "$parent:example.org"}
                               :formatted-body "<strong>formatted</strong>"}]]
                            @calls*))
                     (done)))
            (.catch (fn [err]
                      (is false (.-stack err))
                      (done))))))))

(deftest unwrap-envelope-returns-data-or-throws-error
  (is (= {:status "ok"}
         (broker-client/unwrap-envelope {:ok true :data {:status "ok"}})))
  (try
    (broker-client/unwrap-envelope {:ok false
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
  (let [result (broker-client/parse-sse-chunk
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

(deftest event-stream-close-notifies-caller-with-diagnostics
  (let [res-handlers* (atom {})
        req-handlers* (atom {})
        closed* (atom nil)
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
    (with-redefs [broker-client/http fake-http]
      (broker-client/open-event-stream! {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                         :on-close #(reset! closed* %)}
                                        "client-1"
                                        (fn [_event]))
      ((get @res-handlers* "close"))
      (is (= {:client/id "client-1"
              :stream/connected? true
              :stream/closed? true
              :stream/close-reason "response-close"
              :http/status-code 200}
             (select-keys @closed* [:client/id
                                    :stream/connected?
                                    :stream/closed?
                                    :stream/close-reason
                                    :http/status-code]))))))

(deftest slot-lifecycle-helpers-use-versioned-endpoints
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}]
      (with-redefs [broker-client/request-edn! (fn
                                                  ([method uri body]
                                                   (swap! calls* conj [{} method uri body])
                                                   (js/Promise.resolve {:ok true}))
                                                  ([opts method uri body]
                                                   (swap! calls* conj [opts method uri body])
                                                   (js/Promise.resolve {:ok true})))]
        (-> (js/Promise.all
             (clj->js [(broker-client/acquire-slot! opts "client-1" {:project/id "project" :project/display-name "Project"} ["@op:example.org"])
                       (broker-client/release-slot! opts "client-1" "!slot:example.org" "A")
                       (broker-client/heartbeat! opts "client-1")
                       (broker-client/unregister-client! opts "client-1" "shutdown")
                       (broker-client/update-subscriptions! opts "client-1" ["!project:example.org" "!slot:example.org"])
                       (broker-client/list-slots! opts "project")]))
            (.then (fn [_]
                     (is (= [["POST" "/v1/slots/acquire"
                              {:client/id "client-1"
                               :project {:project/id "project" :project/display-name "Project"}
                               :invite ["@op:example.org"]}]
                             ["POST" "/v1/slots/release"
                              {:client/id "client-1"
                               :room/id "!slot:example.org"
                               :slot "A"}]
                             ["POST" "/v1/clients/client-1/heartbeat" {}]
                             ["DELETE" "/v1/clients/client-1" {:reason "shutdown"}]
                             ["PATCH" "/v1/clients/client-1/subscriptions"
                              {:rooms ["!project:example.org" "!slot:example.org"]}]
                             ["GET" "/v1/slots?project-id=project" nil]]
                            (mapv (fn [[_opts method uri body]] [method uri body]) @calls*)))
                     (done)))
            (.catch (fn [err]
                      (is false (.-stack err))
                      (done))))))))

(deftest client-path-helpers-url-encode-client-and-room-ids
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}
          client-id "matrix-relay-/work/project"
          encoded-client-id "matrix-relay-%2Fwork%2Fproject"
          room-id "!slot:example.org"
          encoded-room-id "!slot%3Aexample.org"]
      (is (= (str "/v1/clients/" encoded-client-id "/events")
             (:path (js->clj (broker-client/event-stream-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                                                  client-id)
                             :keywordize-keys true))))
      (with-redefs [broker-client/request-edn! (fn
                                                  ([method uri body]
                                                   (swap! calls* conj [method uri body])
                                                   (js/Promise.resolve {:ok true}))
                                                  ([_opts method uri body]
                                                   (swap! calls* conj [method uri body])
                                                   (js/Promise.resolve {:ok true})))]
        (-> (js/Promise.all
             (clj->js [(broker-client/heartbeat! opts client-id)
                       (broker-client/unregister-client! opts client-id "shutdown")
                       (broker-client/update-subscriptions! opts client-id [room-id])
                       (broker-client/get-room-delivery-mode! opts client-id room-id)
                       (broker-client/set-room-delivery-mode! opts client-id room-id "steer" "@alice:example.org")]))
            (.then (fn [_]
                     (is (= [["POST" (str "/v1/clients/" encoded-client-id "/heartbeat") {}]
                             ["DELETE" (str "/v1/clients/" encoded-client-id) {:reason "shutdown"}]
                             ["PATCH" (str "/v1/clients/" encoded-client-id "/subscriptions")
                              {:rooms [room-id]}]
                             ["GET" (str "/v1/clients/" encoded-client-id "/rooms/" encoded-room-id "/delivery-mode") nil]
                             ["PUT" (str "/v1/clients/" encoded-client-id "/rooms/" encoded-room-id "/delivery-mode")
                              {:room/default-delivery-mode "steer"
                               :room/default-delivery-mode-updated-by-user "@alice:example.org"}]]
                            @calls*))
                     (done)))
            (.catch (fn [err]
                      (is false (.-stack err))
                      (done))))))))
