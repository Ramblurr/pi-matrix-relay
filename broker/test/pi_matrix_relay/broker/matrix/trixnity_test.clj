(ns pi-matrix-relay.broker.matrix.trixnity-test
  (:require [clojure.test :refer [deftest is testing]]
            [ol.trixnity.event :as event]
            [pi-matrix-relay.broker.matrix.trixnity :as sut]))

(deftest normalized-message-events-mark-bot-sent-events
  (testing "Matrix sync echoes from the bot are marked for extension-side suppression"
    (with-redefs [event/text? (constantly true)
                  event/reaction? (constantly false)
                  event/room-id (constantly "!room:example.org")
                  event/event-id (constantly "$event:example.org")
                  event/sender (constantly "@bot:example.org")
                  event/sender-display-name (constantly "Bot")
                  event/msgtype (constantly "m.text")
                  event/body (constantly "hello")
                  event/relation-event-id (constantly nil)
                  event/url (constantly nil)
                  event/encrypted-file (constantly nil)]
      (is (= true
             (get-in (#'sut/normalized-event ::event "@bot:example.org")
                     [:data :event/sender-is-bot?])))
      (is (= false
             (get-in (#'sut/normalized-event ::event "@other:example.org")
                     [:data :event/sender-is-bot?]))))))
