(ns pi-matrix-relay.broker-client-test
  (:require [cljs.test :refer [async deftest is]]
            [pi-matrix-relay.broker-client :as broker-client]
            [pi-matrix-relay.http :as http]))

(deftest send-message-includes-optional-formatted-body
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}]
      (with-redefs [http/request-edn! (fn
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


(deftest set-typing-posts-client-room-timeout
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}]
      (with-redefs [http/request-edn! (fn
                                         ([method uri body]
                                          (swap! calls* conj [{} method uri body])
                                          (js/Promise.resolve {}))
                                         ([opts method uri body]
                                          (swap! calls* conj [opts method uri body])
                                          (js/Promise.resolve {})))]
        (-> (broker-client/set-typing! opts
                                       "!room:example.org"
                                       true
                                       {:client/id "client-1"
                                        :timeout/ms 45000})
            (.then (fn [_]
                     (is (= [[opts "POST" "/v1/matrix/typing"
                              {:room/id "!room:example.org"
                               :typing true
                               :client/id "client-1"
                               :timeout/ms 45000}]]
                            @calls*))
                     (done)))
            (.catch (fn [err]
                      (is false (.-stack err))
                      (done))))))))

(deftest slot-lifecycle-helpers-use-versioned-endpoints
  (async done
    (let [calls* (atom [])
          opts {:env #js {"XDG_RUNTIME_DIR" "/run/user/1000"}}]
      (with-redefs [http/request-edn! (fn
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
      (with-redefs [http/request-edn! (fn
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
                       (broker-client/set-room-delivery-mode! opts client-id room-id "steer" "@alice:example.org")
                       (broker-client/get-room-prompt-mode! opts client-id room-id)
                       (broker-client/set-room-prompt-mode! opts client-id room-id "commands-only" "@alice:example.org")]))
            (.then (fn [_]
                     (is (= [["POST" (str "/v1/clients/" encoded-client-id "/heartbeat") {}]
                             ["DELETE" (str "/v1/clients/" encoded-client-id) {:reason "shutdown"}]
                             ["PATCH" (str "/v1/clients/" encoded-client-id "/subscriptions")
                              {:rooms [room-id]}]
                             ["GET" (str "/v1/clients/" encoded-client-id "/rooms/" encoded-room-id "/delivery-mode") nil]
                             ["PUT" (str "/v1/clients/" encoded-client-id "/rooms/" encoded-room-id "/delivery-mode")
                              {:room/default-delivery-mode "steer"
                               :room/default-delivery-mode-updated-by-user "@alice:example.org"}]
                             ["GET" (str "/v1/clients/" encoded-client-id "/rooms/" encoded-room-id "/prompt-mode") nil]
                             ["PUT" (str "/v1/clients/" encoded-client-id "/rooms/" encoded-room-id "/prompt-mode")
                              {:room/prompt-mode "commands-only"
                               :room/prompt-mode-updated-by-user "@alice:example.org"}]]
                            @calls*))
                     (done)))
            (.catch (fn [err]
                      (is false (.-stack err))
                      (done))))))))

