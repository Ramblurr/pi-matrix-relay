(ns pi-matrix-relay.broker.matrix.trixnity-test
  (:require [clojure.test :refer [deftest is testing]]
            [missionary.core :as m]
            [ol.trixnity.event :as event]
            [ol.trixnity.key :as key]
            [ol.trixnity.room :as room]
            [ol.trixnity.schemas :as mx]
            [ol.trixnity.space :as space]
            [ol.trixnity.verification :as verification]
            [org.httpkit.client :as http]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.matrix :as matrix]
            [pi-matrix-relay.broker.store :as store]
            [pi-matrix-relay.broker.test-util :as tu]
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

(defn- gateway-config
  [space-config]
  {:matrix {:homeserver-url "https://matrix.example.org"
            :user-id "@bot:example.org"
            :password "secret"
            :operators ["@alice:example.org" "@bob:example.org"]
            :space space-config}})

(deftest existing-space-setup-validates-joined-space-and-child-power
  (let [gateway (sut/gateway (gateway-config {:enabled? true
                                              :mode :existing
                                              :room-id-or-alias "#relay:example.org"})
                             {})
        calls* (atom [])]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [room/join-room (fn [client room-id-or-alias opts]
                                   (swap! calls* conj [:join-room client room-id-or-alias opts])
                                   (m/sp "!space:example.org"))
                  space/get-all-flat (fn [client]
                                       (swap! calls* conj [:get-spaces client])
                                       (m/seed [[{::mx/room-id "!space:example.org"
                                                  ::mx/membership "join"}]]))
                  room/get-power-levels (fn [client room-id]
                                          (swap! calls* conj [:get-power-levels client room-id])
                                          (m/seed [{::mx/user-levels {"@bot:example.org" 75}
                                                    ::mx/event-levels {"m.space.child" 75}}]))]
      (is (= {:space/id "!space:example.org"
              :space/mode :existing}
             (matrix/ensure-space! gateway {})))
      (is (= "!space:example.org" (:space/id @(:runtime* gateway))))
      (is (= [[:join-room :client "#relay:example.org" {::mx/timeout (Duration/ofSeconds 15)}]
              [:get-spaces :client]
              [:get-power-levels :client "!space:example.org"]]
             @calls*)))))

(deftest existing-space-setup-fails-when-bot-cannot-manage-children
  (let [gateway (sut/gateway (gateway-config {:enabled? true
                                              :mode :existing
                                              :room-id-or-alias "!space:example.org"})
                             {})]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [room/join-room (fn [& _] (m/sp "!space:example.org"))
                  space/get-all-flat (fn [_]
                                       (m/seed [[{::mx/room-id "!space:example.org"
                                                  ::mx/membership "join"}]]))
                  room/get-power-levels (fn [_ _]
                                          (m/seed [{::mx/user-levels {"@bot:example.org" 10}
                                                    ::mx/state-default-level 50}]))]
      (let [ex (try
                 (matrix/ensure-space! gateway {})
                 nil
                 (catch clojure.lang.ExceptionInfo ex
                   ex))]
        (is (= :matrix_space_setup_failed (:code (ex-data ex))))
        (is (re-find #"invite/promote" (ex-message ex)))
        (is (= {:space-id "!space:example.org"
                :bot-user-id "@bot:example.org"
                :bot-level 10
                :required-level 50}
               (select-keys (ex-data ex) [:space-id :bot-user-id :bot-level :required-level])))))))

(deftest existing-space-setup-fails-clearly-when-space-cannot-be-joined
  (let [gateway (sut/gateway (gateway-config {:enabled? true
                                              :mode :existing
                                              :room-id-or-alias "#missing:example.org"})
                             {})]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [room/join-room (fn [& _]
                                   (m/sp
                                    (throw (ex-info "M_FORBIDDEN" {:errcode "M_FORBIDDEN"}))))]
      (let [ex (try
                 (matrix/ensure-space! gateway {})
                 nil
                 (catch clojure.lang.ExceptionInfo ex
                   ex))]
        (is (= :matrix_space_setup_failed (:code (ex-data ex))))
        (is (re-find #"Invite the bot" (ex-message ex)))
        (is (= {:room-id-or-alias "#missing:example.org"}
               (select-keys (ex-data ex) [:room-id-or-alias])))))))

(deftest create-space-setup-invites-promotes-and-persists-operators
  (let [conn (tu/test-db-conn)
        created* (atom [])]
    (try
      (let [config (gateway-config {:enabled? true
                                    :mode :create
                                    :name "Pi Relay"})
            gateway-1 (sut/gateway config {})
            gateway-2 (sut/gateway config {})]
        (doseq [gateway [gateway-1 gateway-2]]
          (reset! (:runtime* gateway) {:client :client}))
        (with-redefs [space/create-space (fn [client opts]
                                           (swap! created* conj [:create-space client opts])
                                           (m/sp "!created-space:example.org"))
                      space/get-all-flat (fn [_]
                                           (m/seed [[{::mx/room-id "!created-space:example.org"
                                                      ::mx/membership "join"}]]))
                      room/get-power-levels (fn [_ _]
                                              (m/seed [{::mx/user-levels {"@bot:example.org" 100
                                                                        "@alice:example.org" 100
                                                                        "@bob:example.org" 100}
                                                        ::mx/state-default-level 50}]))]
          (is (= {:space/id "!created-space:example.org"
                  :space/mode :create}
                 (matrix/ensure-space! gateway-1 {:db-conn conn})))
          (is (= {:space/id "!created-space:example.org"
                  :space/mode :create}
                 (matrix/ensure-space! gateway-2 {:db-conn conn})))
          (is (= [{:space-key "default"
                   :room-id "!created-space:example.org"
                   :source :created}]
                 [(select-keys (store/matrix-space @conn "default") [:space-key :room-id :source])]))
          (is (= [[:create-space :client
                   {::mx/room-name "Pi Relay"
                    ::mx/visibility :private
                    ::mx/preset :private-chat
                    ::mx/invite ["@alice:example.org" "@bob:example.org"]
                    ::mx/power-levels {::mx/user-levels {"@bot:example.org" 100
                                                        "@alice:example.org" 100
                                                        "@bob:example.org" 100}}}]]
                 @created*))))
      (finally
        (db/release-conn! conn)))))

(deftest ensure-room-in-space-writes-idempotent-child-relation
  (let [gateway (sut/gateway (gateway-config {:enabled? true
                                              :mode :existing
                                              :room-id-or-alias "!space:example.org"})
                             {})
        calls* (atom [])]
    (reset! (:runtime* gateway) {:client :client
                                 :space/id "!space:example.org"})
    (with-redefs [space/set-child (fn [client space-id child-room-id content opts]
                                    (swap! calls* conj [:set-child client space-id child-room-id content opts])
                                    (m/sp "$child:example.org"))]
      (is (= {:space/id "!space:example.org"
              :room/id "!project-A:example.org"
              :linked? true}
             (matrix/ensure-room-in-space! gateway {:room/id "!project-A:example.org"})))
      (is (= [[:set-child :client "!space:example.org" "!project-A:example.org"
               {::mx/via #{"example.org"}}
               {::mx/timeout (Duration/ofSeconds 10)}]]
             @calls*)))))

(deftest matrix-started-room-verification-requests-are-activated
  (testing "room verification request events must be registered with Trixnity before they appear in status"
    (let [calls* (atom [])
          snapshot {::mx/verification-id "user:!dm:example.org:$request"
                    ::mx/verification-kind :user
                    ::mx/their-user-id "@alice:example.org"
                    ::mx/room-id "!dm:example.org"
                    ::mx/request-event-id "$request"
                    ::mx/timestamp 123
                    ::mx/verification-state {::mx/kind :their-request}}]
      (with-redefs [event/text? (constantly true)
                    event/body (constantly nil)
                    event/room-id (constantly "!dm:example.org")
                    event/event-id (constantly "$request")
                    verification/get-active-user-verification!
                    (fn [client room-id event-id]
                      (swap! calls* conj [client room-id event-id])
                      (m/sp snapshot))]
        (is (= {:verification-id "user:!dm:example.org:$request"
                :verification-kind :user
                :their-user-id "@alice:example.org"
                :room-id "!dm:example.org"
                :request-event-id "$request"
                :timestamp 123
                :verification-state {:kind :their-request}}
               (#'sut/activate-room-verification-request! :client ::event)))
        (is (= [[:client "!dm:example.org" "$request"]]
               @calls*))))))

(deftest ordinary-text-messages-do-not-trigger-room-verification-activation
  (let [calls* (atom [])]
    (with-redefs [event/text? (constantly true)
                  event/body (constantly "hello")
                  verification/get-active-user-verification!
                  (fn [& args]
                    (swap! calls* conj args)
                    (m/sp nil))]
      (is (nil? (#'sut/activate-room-verification-request! :client ::event)))
      (is (= [] @calls*)))))

(deftest recent-room-verification-requests-are-activated-after-joining-invite
  (let [calls* (atom [])
        snapshot {::mx/verification-id "user:!dm:example.org:$request"
                  ::mx/verification-kind :user
                  ::mx/room-id "!dm:example.org"
                  ::mx/request-event-id "$request"
                  ::mx/verification-state {::mx/kind :their-request}}]
    (with-redefs [room/get-last-timeline-events-list
                  (fn [client room-id max-size min-size opts]
                    (swap! calls* conj [:recent client room-id max-size min-size opts])
                    (m/seed [[{:kind :request-event} {:kind :ordinary-event}]]))
                  event/text? (fn [ev]
                                (= {:kind :request-event} ev))
                  event/body (constantly nil)
                  event/room-id (constantly "!dm:example.org")
                  event/event-id (fn [ev]
                                   (case (:kind ev)
                                     :request-event "$request"
                                     :ordinary-event "$ordinary"))
                  verification/get-active-user-verification!
                  (fn [client room-id event-id]
                    (swap! calls* conj [:activate client room-id event-id])
                    (m/sp snapshot))]
      (is (= [{:verification-id "user:!dm:example.org:$request"
               :verification-kind :user
               :room-id "!dm:example.org"
               :request-event-id "$request"
               :verification-state {:kind :their-request}}]
             (#'sut/activate-recent-room-verification-requests! :client "!dm:example.org")))
      (is (= [[:recent :client "!dm:example.org" 20 1 {::mx/decryption-timeout (Duration/ofSeconds 8)
                                                        ::mx/fetch-timeout (Duration/ofSeconds 8)}]
              [:activate :client "!dm:example.org" "$request"]]
             @calls*)))))

(deftest direct-invites-are-joined-and-scanned-for-verification-requests
  (let [calls* (atom [])
        invite {::mx/room-id "!dm:example.org"
                ::mx/membership "invite"
                ::mx/is-direct true}]
    (with-redefs [room/join-room (fn [client room-id opts]
                                    (swap! calls* conj [:join client room-id opts])
                                    (m/sp room-id))
                  sut/activate-recent-room-verification-requests!
                  (fn [client room-id]
                    (swap! calls* conj [:activate-recent client room-id])
                    [{:verification-id "verification-1"}])]
      (is (= {:room/id "!dm:example.org"
              :joined? true
              :activated [{:verification-id "verification-1"}]}
             (#'sut/join-direct-invite! :client invite)))
      (is (= [[:join :client "!dm:example.org" {::mx/timeout (Duration/ofSeconds 15)}]
              [:activate-recent :client "!dm:example.org"]]
             @calls*)))))


(deftest verification-actions-use-public-trixnity-verification-api
  (let [gateway (sut/gateway (gateway-config nil) {})
        calls* (atom [])
        snapshot {::mx/verification-id "device:@alice:example.org:DEVICE:txn"
                  ::mx/verification-kind :device
                  ::mx/their-user-id "@alice:example.org"
                  ::mx/their-device-id "DEVICE"
                  ::mx/timestamp 123
                  ::mx/verification-state {::mx/kind :ready}}]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [verification/start-device-verification!
                  (fn [client user-id device-id]
                    (swap! calls* conj [:start-device client user-id device-id])
                    (m/sp snapshot))
                  verification/start-user-verification!
                  (fn [client user-id]
                    (swap! calls* conj [:start-user client user-id])
                    (m/sp (assoc snapshot ::mx/verification-kind :user)))
                  verification/accept!
                  (fn [client verification-id]
                    (swap! calls* conj [:accept client verification-id])
                    (m/sp snapshot))
                  verification/start-sas!
                  (fn [client verification-id]
                    (swap! calls* conj [:start-sas client verification-id])
                    (m/sp snapshot))
                  verification/confirm!
                  (fn [client verification-id]
                    (swap! calls* conj [:confirm client verification-id])
                    (m/sp snapshot))
                  verification/no-match!
                  (fn [client verification-id]
                    (swap! calls* conj [:no-match client verification-id])
                    (m/sp snapshot))
                  verification/cancel!
                  (fn [client verification-id reason]
                    (swap! calls* conj [:cancel client verification-id reason])
                    (m/sp snapshot))
                  verification/status
                  (fn [client]
                    (swap! calls* conj [:status client])
                    [snapshot])]
      (let [device-result (matrix/verification-start! gateway {:user/id "@alice:example.org"
                                                               :device/id "DEVICE"})
            user-result (matrix/verification-start! gateway {:user/id "@alice:example.org"})
            accept-result (matrix/verification-accept! gateway "verification-1")
            start-sas-result (matrix/verification-start-sas! gateway "verification-1")
            confirm-result (matrix/verification-confirm! gateway "verification-1")
            no-match-result (matrix/verification-no-match! gateway "verification-1")
            cancel-result (matrix/verification-cancel! gateway "verification-1")
            status-result (matrix/verification-status gateway)]
        (is (= {:device {:verification-id "device:@alice:example.org:DEVICE:txn"
                         :verification-kind :device
                         :their-user-id "@alice:example.org"
                         :their-device-id "DEVICE"
                         :timestamp 123
                         :verification-state {:kind :ready}}
                :user {:verification-id "device:@alice:example.org:DEVICE:txn"
                       :verification-kind :user
                       :their-user-id "@alice:example.org"
                       :their-device-id "DEVICE"
                       :timestamp 123
                       :verification-state {:kind :ready}}
                :accept {:verification-id "device:@alice:example.org:DEVICE:txn"
                         :verification-kind :device
                         :their-user-id "@alice:example.org"
                         :their-device-id "DEVICE"
                         :timestamp 123
                         :verification-state {:kind :ready}}
                :start-sas {:verification-id "device:@alice:example.org:DEVICE:txn"
                            :verification-kind :device
                            :their-user-id "@alice:example.org"
                            :their-device-id "DEVICE"
                            :timestamp 123
                            :verification-state {:kind :ready}}
                :confirm {:verification-id "device:@alice:example.org:DEVICE:txn"
                          :verification-kind :device
                          :their-user-id "@alice:example.org"
                          :their-device-id "DEVICE"
                          :timestamp 123
                          :verification-state {:kind :ready}}
                :no-match {:verification-id "device:@alice:example.org:DEVICE:txn"
                           :verification-kind :device
                           :their-user-id "@alice:example.org"
                           :their-device-id "DEVICE"
                           :timestamp 123
                           :verification-state {:kind :ready}}
                :cancel {:verification-id "device:@alice:example.org:DEVICE:txn"
                         :verification-kind :device
                         :their-user-id "@alice:example.org"
                         :their-device-id "DEVICE"
                         :timestamp 123
                         :verification-state {:kind :ready}}
                :status {:verifications [{:verification-id "device:@alice:example.org:DEVICE:txn"
                                           :verification-kind :device
                                           :their-user-id "@alice:example.org"
                                           :their-device-id "DEVICE"
                                           :timestamp 123
                                           :verification-state {:kind :ready}}]}
                :calls [[:start-device :client "@alice:example.org" "DEVICE"]
                        [:start-user :client "@alice:example.org"]
                        [:accept :client "verification-1"]
                        [:start-sas :client "verification-1"]
                        [:confirm :client "verification-1"]
                        [:no-match :client "verification-1"]
                        [:cancel :client "verification-1" nil]
                        [:status :client]]}
               {:device device-result
                :user user-result
                :accept accept-result
                :start-sas start-sas-result
                :confirm confirm-result
                :no-match no-match-result
                :cancel cancel-result
                :status status-result
                :calls @calls*}))))))

(deftest verification-bootstrap-uses-public-trixnity-key-api-and-broker-password
  (let [gateway (sut/gateway {:matrix {:homeserver-url "https://matrix.example.org"
                                       :user-id "@bot:example.org"
                                       :password "bot-password"}}
                             {})
        calls* (atom [])
        snapshot {::mx/kind "success"
                  ::mx/recovery-key "RECOVERY"
                  ::mx/uia {::mx/kind "success"}}]
    (reset! (:runtime* gateway) {:client :client})
    (with-redefs [key/bootstrap-cross-signing!
                  (fn [client opts]
                    (swap! calls* conj [:bootstrap client opts])
                    (m/sp snapshot))
                  key/bootstrap-cross-signing-from-passphrase!
                  (fn [client passphrase opts]
                    (swap! calls* conj [:bootstrap-passphrase client passphrase opts])
                    (m/sp snapshot))]
      (is (= {:kind "success"
              :recovery-key "RECOVERY"
              :uia {:kind "success"}}
             (matrix/verification-bootstrap! gateway {})))
      (is (= {:kind "success"
              :recovery-key "RECOVERY"
              :uia {:kind "success"}}
             (matrix/verification-bootstrap! gateway {:passphrase "storage-passphrase"})))
      (is (= [[:bootstrap :client {::mx/password "bot-password"
                                   ::mx/user-id "@bot:example.org"}]
              [:bootstrap-passphrase :client "storage-passphrase" {::mx/password "bot-password"
                                                                    ::mx/user-id "@bot:example.org"}]]
             @calls*)))))
