(ns pi-matrix-relay.broker.matrix.trixnity-test
  (:require [clojure.test :refer [deftest is testing]]
            [missionary.core :as m]
            [ol.trixnity.event :as event]
            [ol.trixnity.room :as room]
            [ol.trixnity.schemas :as mx]
            [org.httpkit.client :as http]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.matrix.trixnity :as sut])
  (:import [java.time Duration]))

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

(defn- java-map
  [entries]
  (doto (java.util.LinkedHashMap.)
    (as-> m
      (doseq [[k v] entries]
        (.put m k v)))))

(deftest ensure-users-power-level-uses-trixnity-room-power-level-api
  (let [gateway (sut/gateway {:matrix {:homeserver-url "https://matrix.example.org"
                                       :user-id "@bot:example.org"
                                       :password "secret"}}
                             {})
        calls* (atom [])
        existing-power-levels (java-map [[::mx/ban-level 50]
                                         [::mx/raw ::upstream-raw-power-levels]
                                         [::mx/event-levels (java-map [["m.room.name" 50]])]
                                         [::mx/user-levels (java-map [["@existing:example.org" 50]])]])]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [http/request (fn [& _]
                                 (throw (ex-info "raw Matrix HTTP bypass should not be used" {})))
                  room/get-power-levels (fn [client room-id]
                                          (swap! calls* conj [:get-power-levels client room-id])
                                          (m/seed [existing-power-levels]))
                  room/set-power-levels (fn [client room-id power-levels opts]
                                          (swap! calls* conj [:set-power-levels client room-id power-levels opts])
                                          (m/sp "$power-levels:example.org"))]
      (is (= {:room/id "!room:example.org"
              :users ["@alice:example.org" "@bob:example.org"]
              :level 100}
             (matrix/ensure-users-power-level! gateway
                                               {:room/id "!room:example.org"
                                                :users ["@alice:example.org" "@bob:example.org"]})))
      (is (= [[:get-power-levels :client "!room:example.org"]
              [:set-power-levels :client "!room:example.org"
               {::mx/ban-level 50
                ::mx/event-levels {"m.room.name" 50}
                ::mx/user-levels {"@existing:example.org" 50
                                  "@alice:example.org" 100
                                  "@bob:example.org" 100}}
               {::mx/timeout (Duration/ofSeconds 10)}]]
             @calls*)))))

(deftest ensure-users-power-level-skips-set-when-users-already-have-level
  (let [gateway (sut/gateway {:matrix {:homeserver-url "https://matrix.example.org"
                                       :user-id "@bot:example.org"
                                       :password "secret"}}
                             {})
        calls* (atom [])
        existing-power-levels {::mx/user-levels {"@alice:example.org" 50}}]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [http/request (fn [& _]
                                 (throw (ex-info "raw Matrix HTTP bypass should not be used" {})))
                  room/get-power-levels (fn [client room-id]
                                          (swap! calls* conj [:get-power-levels client room-id])
                                          (m/seed [existing-power-levels]))
                  room/set-power-levels (fn [& args]
                                          (swap! calls* conj (into [:set-power-levels] args))
                                          (m/sp "$power-levels:example.org"))]
      (is (= {:room/id "!room:example.org"
              :users ["@alice:example.org"]
              :level 50}
             (matrix/ensure-users-power-level! gateway
                                               {:room/id "!room:example.org"
                                                :users ["@alice:example.org"]
                                                :level 50})))
      (is (= [[:get-power-levels :client "!room:example.org"]]
             @calls*)))))
