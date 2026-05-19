(ns pi-matrix-relay.broker.repl-test
  (:require [clojure.test :refer [deftest is]]
            [missionary.core :as m]
            [ol.trixnity.event :as event]
            [ol.trixnity.schemas :as mx]
            [ol.trixnity.verification :as verification]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.repl :as sut]))

(deftest verification-status-delegates-to-gateway
  (with-redefs [matrix/verification-status (fn [gateway]
                                             {:gateway gateway
                                              :verifications []})]
    (is (= {:gateway :gateway
            :verifications []}
           (sut/verification-status :gateway)))))

(deftest verification-actions-delegate-to-gateway
  (let [calls* (atom [])]
    (with-redefs [matrix/verification-start! (fn [gateway request]
                                               (swap! calls* conj [:start gateway request])
                                               {:started request})
                  matrix/verification-accept! (fn [gateway verification-id]
                                                (swap! calls* conj [:accept gateway verification-id])
                                                {:accepted verification-id})
                  matrix/verification-start-sas! (fn [gateway verification-id]
                                                   (swap! calls* conj [:start-sas gateway verification-id])
                                                   {:sas verification-id})
                  matrix/verification-confirm! (fn [gateway verification-id]
                                                 (swap! calls* conj [:confirm gateway verification-id])
                                                 {:confirmed verification-id})
                  matrix/verification-cancel! (fn [gateway verification-id]
                                                (swap! calls* conj [:cancel gateway verification-id])
                                                {:cancelled verification-id})]
      (is (= {:started {:user/id "@alice:example.org"}}
             (sut/start-user-verification! :gateway "@alice:example.org")))
      (is (= {:started {:user/id "@alice:example.org"
                        :device/id "DEVICE"}}
             (sut/start-device-verification! :gateway "@alice:example.org" "DEVICE")))
      (is (= {:accepted "verification-1"}
             (sut/accept! :gateway "verification-1")))
      (is (= {:sas "verification-1"}
             (sut/start-sas! :gateway "verification-1")))
      (is (= {:confirmed "verification-1"}
             (sut/confirm! :gateway "verification-1")))
      (is (= {:cancelled "verification-1"}
             (sut/cancel! :gateway "verification-1")))
      (is (= [[:start :gateway {:user/id "@alice:example.org"}]
              [:start :gateway {:user/id "@alice:example.org"
                                :device/id "DEVICE"}]
              [:accept :gateway "verification-1"]
              [:start-sas :gateway "verification-1"]
              [:confirm :gateway "verification-1"]
              [:cancel :gateway "verification-1"]]
             @calls*)))))

(deftest activate-room-request-uses-public-trixnity-verification-api
  (let [calls* (atom [])
        snapshot {::mx/verification-id "user:!dm:example.org:$request"
                  ::mx/verification-kind :user
                  ::mx/room-id "!dm:example.org"
                  ::mx/request-event-id "$request"
                  ::mx/verification-state {::mx/kind :their-request}}]
    (with-redefs [verification/get-active-user-verification! (fn [client room-id event-id]
                                                               (swap! calls* conj [client room-id event-id])
                                                               (m/sp snapshot))]
      (is (= {:verification-id "user:!dm:example.org:$request"
              :verification-kind :user
              :room-id "!dm:example.org"
              :request-event-id "$request"
              :verification-state {:kind :their-request}}
             (sut/activate-room-request! :client "!dm:example.org" "$request")))
      (is (= [[:client "!dm:example.org" "$request"]]
             @calls*)))))

(deftest summarize-event-marks-room-verification-request-candidates
  (with-redefs [event/room-id (constantly "!dm:example.org")
                event/event-id (constantly "$request")
                event/type (constantly "m.room.message")
                event/sender (constantly "@alice:example.org")
                event/body (constantly nil)
                event/msgtype (constantly nil)
                event/text? (constantly true)]
    (is (= {:room/id "!dm:example.org"
            :event/id "$request"
            :event/type "m.room.message"
            :event/sender "@alice:example.org"
            :event/body nil
            :message/type nil
            :verification/request-candidate? true}
           (sut/summarize-event ::event)))))

(deftest activate-recent-room-requests-activates-candidates
  (let [calls* (atom [])
        candidate {:room/id "!dm:example.org"
                   :event/id "$request"
                   :verification/request-candidate? true}]
    (with-redefs [sut/matrix-client (fn [gateway]
                                      (swap! calls* conj [:client gateway])
                                      :client)
                  sut/verification-request-candidates (fn [gateway opts]
                                                        (swap! calls* conj [:candidates gateway opts])
                                                        [candidate])
                  sut/activate-room-request! (fn [client room-id event-id]
                                               (swap! calls* conj [:activate client room-id event-id])
                                               {:verification-id "verification-1"})]
      (is (= [(assoc candidate :activation/result {:verification-id "verification-1"})]
             (sut/activate-recent-room-requests! :gateway {:limit 5})))
      (is (= [[:client :gateway]
              [:candidates :gateway {:limit 5}]
              [:activate :client "!dm:example.org" "$request"]]
             @calls*)))))

(deftest verification-request-candidates-pass-room-timeout-to-timeline-scan
  (let [calls* (atom [])
        candidate {:room/id "!dm:example.org"
                   :event/id "$request"
                   :verification/request-candidate? true}]
    (with-redefs [sut/matrix-client (fn [gateway]
                                      (swap! calls* conj [:client gateway])
                                      :client)
                  matrix/list-rooms! (fn [gateway]
                                       (swap! calls* conj [:rooms gateway])
                                       [{:room/id "!dm:example.org"}])
                  sut/recent-events (fn [client room-id opts]
                                      (swap! calls* conj [:recent client room-id opts])
                                      [candidate])]
      (is (= [candidate]
             (sut/verification-request-candidates :gateway {:limit 7
                                                             :room-timeout-ms 123})))
      (is (= [[:client :gateway]
              [:rooms :gateway]
              [:recent :client "!dm:example.org" {:limit 7
                                                   :timeout-ms 123}]]
             @calls*)))))
