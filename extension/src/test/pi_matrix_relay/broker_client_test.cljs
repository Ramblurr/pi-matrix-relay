(ns pi-matrix-relay.broker-client-test
  (:require [cljs.test :refer [deftest is testing]]
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
