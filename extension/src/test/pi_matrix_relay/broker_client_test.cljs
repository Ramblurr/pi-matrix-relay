(ns pi-matrix-relay.broker-client-test
  (:require [cljs.test :refer [async deftest is testing]]
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
          :headers {:Accept "application/json"}}
         (js->clj (broker-client/request-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                                 "GET"
                                                 "/v1/health"
                                                 nil)
                  :keywordize-keys true)))
  (is (= {:Accept "application/json"
          :Content-Type "application/json"
          :Content-Length 11}
         (get (js->clj (broker-client/request-options #js {"XDG_RUNTIME_DIR" "/run/user/1000"}
                                                       "POST"
                                                       "/v1/matrix/messages"
                                                       "{\"ok\":true}")
                    :keywordize-keys true)
              :headers))))

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
                     "data: {\"type\":\"matrix.message\",\"event\":{\"text\":\"hello\"}}\n\n"
                     "id: evt-2\n"))]
    (is (= [{:id "evt-1"
             :event "matrix.message"
             :data {:type "matrix.message"
                    :event {:text "hello"}}}]
           (:events result)))
    (is (= "id: evt-2\n" (:buffer result)))))

(deftest slot-lifecycle-helpers-use-versioned-endpoints
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}]
      (with-redefs [broker-client/request-json! (fn
                                                  ([method uri body]
                                                   (swap! calls* conj [{} method uri body])
                                                   (js/Promise.resolve {:ok true}))
                                                  ([opts method uri body]
                                                   (swap! calls* conj [opts method uri body])
                                                   (js/Promise.resolve {:ok true})))]
        (-> (js/Promise.all
             (clj->js [(broker-client/acquire-slot! opts "client-1" {:id "project" :displayName "Project"} ["@op:example.org"])
                       (broker-client/release-slot! opts "client-1" "!slot:example.org" "A")
                       (broker-client/heartbeat! opts "client-1")
                       (broker-client/unregister-client! opts "client-1" "shutdown")
                       (broker-client/update-subscriptions! opts "client-1" ["!project:example.org" "!slot:example.org"])]))
            (.then (fn [_]
                     (is (= [["POST" "/v1/slots/acquire"
                              {:clientId "client-1"
                               :project {:id "project" :displayName "Project"}
                               :invite ["@op:example.org"]}]
                             ["POST" "/v1/slots/release"
                              {:clientId "client-1"
                               :roomId "!slot:example.org"
                               :slot "A"}]
                             ["POST" "/v1/clients/client-1/heartbeat" {}]
                             ["DELETE" "/v1/clients/client-1" {:reason "shutdown"}]
                             ["PATCH" "/v1/clients/client-1/subscriptions"
                              {:rooms ["!project:example.org" "!slot:example.org"]}]]
                            (mapv (fn [[_opts method uri body]] [method uri body]) @calls*)))
                     (done)))
            (.catch (fn [err]
                      (is false (.-stack err))
                      (done))))))))
