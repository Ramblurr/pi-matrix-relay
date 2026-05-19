(ns pi-matrix-relay.broker.store-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.store :as store]
            [pi-matrix-relay.broker.test-util :as tu]))

(defn with-conn
  [f]
  (let [conn (tu/test-db-conn)]
    (try
      (f conn)
      (finally
        (db/release-conn! conn)))))

(defn room-ids
  [conn]
  (set (d/q '[:find [?room-id ...]
              :where [_ :room/id ?room-id]]
            @conn)))

(defn subscribed-room-ids
  [conn client-id]
  (set (d/q '[:find [?room-id ...]
              :in $ ?client-id
              :where
              [?client :client/instance-id ?client-id]
              [?client :client/subscribed-room ?room]
              [?room :room/id ?room-id]]
            @conn client-id)))

(deftest clients-subscriptions-and-room-authorization-live-in-datahike
  (with-conn
    (fn [conn]
      (testing "client registration persists subscriptions and uses the instance id as the client id"
        (let [registration (store/register-client!
                            conn
                            {:now-ms 1000
                             :heartbeat-seconds 30
                             :global-operators ["@operator:example.org"]}
                            {:client/instance-id "instance-1"
                             :protocol/version 1
                             :project {:project/id "project" :project/root "/work/project" :project/display-name "Project"}
                             :metadata {:piSessionName "main"}
                             :subscriptions {:rooms ["!project:example.org"]}})]
          (is (= {:client-id "instance-1"
                  :heartbeat-seconds 30
                  :global-operators ["@operator:example.org"]}
                 (select-keys registration [:client-id :heartbeat-seconds :global-operators])))
          (is (= {:canonical-rooms #{"!project:example.org"}
                  :subscriptions #{"!project:example.org"}
                  :project-room true
                  :unknown false}
                 {:canonical-rooms (room-ids conn)
                  :subscriptions (subscribed-room-ids conn "instance-1")
                  :project-room (store/known-room-for-client? @conn "instance-1" "!project:example.org")
                  :unknown (store/known-room-for-client? @conn "instance-1" "!other:example.org")}))))
      (testing "subscription replacement removes room refs without deleting canonical rooms"
        (store/update-subscriptions! conn "instance-1" ["!other:example.org"])
        (is (= {:canonical-rooms #{"!project:example.org" "!other:example.org"}
                :subscriptions #{"!other:example.org"}
                :old false
                :new true}
               {:canonical-rooms (room-ids conn)
                :subscriptions (subscribed-room-ids conn "instance-1")
                :old (store/known-room-for-client? @conn "instance-1" "!project:example.org")
                :new (store/known-room-for-client? @conn "instance-1" "!other:example.org")}))))))

(deftest ensure-room-preserves-persisted-default-delivery-mode
  (with-conn
    (fn [conn]
      (store/register-client! conn {:now-ms 1000}
                              {:client/instance-id "client-1"
                               :protocol/version 1
                               :project {:project/id "project"}
                               :subscriptions {:rooms ["!room:example.org"]}})
      (store/set-room-default-delivery-mode! conn {:client-id "client-1"
                                                   :room-id "!room:example.org"
                                                   :default-delivery-mode :steer
                                                   :updated-by-user "@alice:example.org"
                                                   :now-ms 2000})
      (is (= {:room/id "!room:example.org"
              :room/default-delivery-mode :steer}
             (store/ensure-room! conn "!room:example.org")))
      (is (= :steer (store/room-default-delivery-mode @conn "!room:example.org"))))))

(deftest slot-reservations-use-transaction-time-state-and-cas-lifecycle
  (with-conn
    (fn [conn]
      (doseq [client-id ["client-1" "client-2"]]
        (store/register-client! conn {:now-ms 1000}
                                {:client/instance-id client-id
                                 :protocol/version 1
                                 :project {:project/id "project"}}))
      (testing "two reservations made before Matrix room creation occupy A then B"
        (let [reservation-1 (store/reserve-slot! conn {:now-ms 1100}
                                                 {:client-id "client-1"
                                                  :project {:project/id "project"}})
              reservation-2 (store/reserve-slot! conn {:now-ms 1200}
                                                 {:client-id "client-2"
                                                  :project {:project/id "project"}})]
          (is (= ["A" "B"] (mapv :slot [reservation-1 reservation-2])))
          (let [lease-1 (store/complete-slot-reservation!
                         conn
                         {:now-ms 1300
                          :lease-id (:lease-id reservation-1)
                          :reservation-id (:reservation-id reservation-1)
                          :client-id "client-1"
                          :room-id "!project-A:example.org"
                          :room-name "project-A"})
                lease-2 (store/complete-slot-reservation!
                         conn
                         {:now-ms 1400
                          :lease-id (:lease-id reservation-2)
                          :reservation-id (:reservation-id reservation-2)
                          :client-id "client-2"
                          :room-id "!project-B:example.org"
                          :room-name "project-B"})]
            (is (= [{:slot "A" :state :leased :room-id "!project-A:example.org" :room-name "project-A"}
                    {:slot "B" :state :leased :room-id "!project-B:example.org" :room-name "project-B"}]
                   (mapv #(select-keys % [:slot :state :room-id :room-name]) [lease-1 lease-2])))
            (is (= #{["A" "!project-A:example.org" "project-A"]
                     ["B" "!project-B:example.org" "project-B"]}
                   (set (d/q '[:find ?slot ?room-id ?room-name
                               :where
                               [?lease :lease/slot ?slot]
                               [?lease :lease/slot-room ?slot-room]
                               [?slot-room :slot-room/name ?room-name]
                               [?slot-room :slot-room/room ?room]
                               [?room :room/id ?room-id]]
                             @conn)))))))
      (testing "release uses a state CAS and frees the slot for reuse"
        (is (= {:released true}
               (store/release-slot! conn {:now-ms 1500 :client-id "client-1" :slot "A"})))
        (let [reservation-3 (store/reserve-slot! conn {:now-ms 1600}
                                                 {:client-id "client-1"
                                                  :project {:project/id "project"}})]
          (is (= "A" (:slot reservation-3))))))))

(deftest slot-reservations-prefer-most-recently-released-project-slot
  (with-conn
    (fn [conn]
      (doseq [client-id ["client-a" "client-b" "client-c"]]
        (store/register-client! conn {:now-ms 1000}
                                {:client/instance-id client-id
                                 :protocol/version 1
                                 :project {:project/id "project"}}))
      (let [reservation-a (store/reserve-slot! conn {:now-ms 1100}
                                               {:client-id "client-a"
                                                :project {:project/id "project"}})
            reservation-b (store/reserve-slot! conn {:now-ms 1200}
                                               {:client-id "client-b"
                                                :project {:project/id "project"}})]
        (store/complete-slot-reservation! conn {:now-ms 1300
                                                :lease-id (:lease-id reservation-a)
                                                :reservation-id (:reservation-id reservation-a)
                                                :client-id "client-a"
                                                :room-id "!project-A:example.org"
                                                :room-name "project-A"})
        (store/complete-slot-reservation! conn {:now-ms 1400
                                                :lease-id (:lease-id reservation-b)
                                                :reservation-id (:reservation-id reservation-b)
                                                :client-id "client-b"
                                                :room-id "!project-B:example.org"
                                                :room-name "project-B"})
        (is (= {:released true}
               (store/release-slot! conn {:now-ms 1500 :client-id "client-a" :slot "A"})))
        (is (= {:released true}
               (store/release-slot! conn {:now-ms 1600 :client-id "client-b" :slot "B"})))
        (let [reservation-c (store/reserve-slot! conn {:now-ms 1700}
                                                 {:client-id "client-c"
                                                  :project {:project/id "project"}})]
          (is (= "B" (:slot reservation-c))))))))

(deftest slot-reservation-recent-release-preference-is-project-scoped
  (with-conn
    (fn [conn]
      (doseq [[client-id project-id] [["project-client" "project"]
                                      ["new-project-client" "project"]
                                      ["other-client-a" "other"]
                                      ["other-client-b" "other"]]]
        (store/register-client! conn {:now-ms 1000}
                                {:client/instance-id client-id
                                 :protocol/version 1
                                 :project {:project/id project-id}}))
      (let [project-reservation (store/reserve-slot! conn {:now-ms 1100}
                                                     {:client-id "project-client"
                                                      :project {:project/id "project"}})
            other-reservation-a (store/reserve-slot! conn {:now-ms 1200}
                                                     {:client-id "other-client-a"
                                                      :project {:project/id "other"}})
            other-reservation-b (store/reserve-slot! conn {:now-ms 1300}
                                                     {:client-id "other-client-b"
                                                      :project {:project/id "other"}})]
        (store/complete-slot-reservation! conn {:now-ms 1400
                                                :lease-id (:lease-id project-reservation)
                                                :reservation-id (:reservation-id project-reservation)
                                                :client-id "project-client"
                                                :room-id "!project-A:example.org"
                                                :room-name "project-A"})
        (store/complete-slot-reservation! conn {:now-ms 1500
                                                :lease-id (:lease-id other-reservation-a)
                                                :reservation-id (:reservation-id other-reservation-a)
                                                :client-id "other-client-a"
                                                :room-id "!other-A:example.org"
                                                :room-name "other-A"})
        (store/complete-slot-reservation! conn {:now-ms 1600
                                                :lease-id (:lease-id other-reservation-b)
                                                :reservation-id (:reservation-id other-reservation-b)
                                                :client-id "other-client-b"
                                                :room-id "!other-B:example.org"
                                                :room-name "other-B"})
        (store/release-slot! conn {:now-ms 1700 :client-id "project-client" :slot "A"})
        (store/release-slot! conn {:now-ms 1800 :client-id "other-client-b" :slot "B"})
        (let [reservation (store/reserve-slot! conn {:now-ms 1900}
                                               {:client-id "new-project-client"
                                                :project {:project/id "project"}})]
          (is (= "A" (:slot reservation))))))))

(deftest slot-reservations-do-not-reuse-slot-with-suspect-active-lease
  (with-conn
    (fn [conn]
      (doseq [client-id ["client-a" "client-b" "client-c" "client-d"]]
        (store/register-client! conn {:now-ms 1000}
                                {:client/instance-id client-id
                                 :protocol/version 1
                                 :project {:project/id "project"}}))
      (let [reservation-a (store/reserve-slot! conn {:now-ms 1100}
                                               {:client-id "client-a"
                                                :project {:project/id "project"}})
            reservation-b (store/reserve-slot! conn {:now-ms 1200}
                                               {:client-id "client-b"
                                                :project {:project/id "project"}})]
        (store/complete-slot-reservation! conn {:now-ms 1300
                                                :lease-id (:lease-id reservation-a)
                                                :reservation-id (:reservation-id reservation-a)
                                                :client-id "client-a"
                                                :room-id "!project-A:example.org"
                                                :room-name "project-A"})
        (store/complete-slot-reservation! conn {:now-ms 1400
                                                :lease-id (:lease-id reservation-b)
                                                :reservation-id (:reservation-id reservation-b)
                                                :client-id "client-b"
                                                :room-id "!project-B:example.org"
                                                :room-name "project-B"})
        (store/release-slot! conn {:now-ms 1500 :client-id "client-b" :slot "B"})
        (let [reservation-c (store/reserve-slot! conn {:now-ms 1600}
                                                 {:client-id "client-c"
                                                  :project {:project/id "project"}})]
          (store/complete-slot-reservation! conn {:now-ms 1700
                                                  :lease-id (:lease-id reservation-c)
                                                  :reservation-id (:reservation-id reservation-c)
                                                  :client-id "client-c"
                                                  :room-id "!project-B:example.org"
                                                  :room-name "project-B"})
          (store/mark-client-suspect! conn "client-c" 1800)
          (let [reservation-d (store/reserve-slot! conn {:now-ms 1900}
                                                   {:client-id "client-d"
                                                    :project {:project/id "project"}})]
            (is (= "C" (:slot reservation-d)))))))))

(deftest reserving-slot-for-same-client-replaces-existing-active-lease
  (with-conn
    (fn [conn]
      (store/register-client! conn {:now-ms 1000}
                              {:client/instance-id "client-1"
                               :protocol/version 1
                               :project {:project/id "project"}})
      (let [reservation-1 (store/reserve-slot! conn {:now-ms 1100}
                                               {:client-id "client-1"
                                                :project {:project/id "project"}})]
        (store/complete-slot-reservation! conn {:now-ms 1200
                                                :lease-id (:lease-id reservation-1)
                                                :reservation-id (:reservation-id reservation-1)
                                                :client-id "client-1"
                                                :room-id "!project-A:example.org"
                                                :room-name "project-A"})
        (let [reservation-2 (store/reserve-slot! conn {:now-ms 1300}
                                                 {:client-id "client-1"
                                                  :project {:project/id "project"}})]
          (is (= "A" (:slot reservation-2)))
          (is (= [{:slot "A" :state :reserved}]
                 (mapv #(select-keys % [:slot :state])
                       (:slots (store/list-slots @conn "project")))))
          (is (= [{:slot "A" :state :released :release-reason :replaced}
                  {:slot "A" :state :reserved}]
                 (mapv #(select-keys % [:slot :state :release-reason])
                       (sort-by :reserved-at
                                (filter #(= "A" (:slot %))
                                        (mapv (fn [lease-id]
                                                (#'store/lease-by-id @conn lease-id))
                                              (d/q '[:find [?lease-id ...]
                                                     :where [?lease :lease/id ?lease-id]]
                                                   @conn))))))))))))

(deftest stale-and-suspect-lease-transitions-are-cas-backed
  (with-conn
    (fn [conn]
      (doseq [client-id ["client-1" "client-2"]]
        (store/register-client! conn {:now-ms 1000}
                                {:client/instance-id client-id
                                 :protocol/version 1
                                 :project {:project/id "project"}}))
      (let [r1 (store/reserve-slot! conn {:now-ms 1000} {:client-id "client-1" :project {:project/id "project"}})
            r2 (store/reserve-slot! conn {:now-ms 1000} {:client-id "client-2" :project {:project/id "project"}})]
        (store/complete-slot-reservation! conn {:now-ms 1000
                                                :lease-id (:lease-id r1)
                                                :reservation-id (:reservation-id r1)
                                                :client-id "client-1"
                                                :room-id "!slot-a:example.org"
                                                :room-name "project-A"})
        (store/complete-slot-reservation! conn {:now-ms 99000
                                                :lease-id (:lease-id r2)
                                                :reservation-id (:reservation-id r2)
                                                :client-id "client-2"
                                                :room-id "!slot-b:example.org"
                                                :room-name "project-B"}))
      (store/mark-client-suspect! conn "client-1" 2000)
      (is (= [{:slot "A" :state :suspect}
              {:slot "B" :state :leased}]
             (mapv #(select-keys % [:slot :state])
                   (:slots (store/list-slots @conn "project")))))
      (let [stale (store/mark-stale-leases! conn 100000 30 3)]
        (is (= [{:slot "A" :state :suspect :room-id "!slot-a:example.org"}]
               (mapv #(select-keys % [:slot :state :room-id]) stale)))
        (is (= [{:slot "A" :state :released}
                {:slot "B" :state :leased}]
               (mapv #(select-keys % [:slot :state])
                     (:slots (store/list-slots @conn "project")))))))))

(deftest room-default-delivery-mode-is-persisted-and-authorized
  (with-conn
    (fn [conn]
      (store/register-client! conn {:now-ms 1000}
                              {:client/instance-id "client-1"
                               :protocol/version 1
                               :project {:project/id "project"}
                               :subscriptions {:rooms ["!project:example.org"]}})
      (store/register-client! conn {:now-ms 1000}
                              {:client/instance-id "client-2"
                               :protocol/version 1
                               :project {:project/id "project"}})
      (let [reservation (store/reserve-slot! conn {:now-ms 1100}
                                             {:client-id "client-2"
                                              :project {:project/id "project"}})]
        (store/complete-slot-reservation! conn {:now-ms 1200
                                                :lease-id (:lease-id reservation)
                                                :reservation-id (:reservation-id reservation)
                                                :client-id "client-2"
                                                :room-id "!slot:example.org"
                                                :room-name "project-A"}))
      (testing "subscribed room writes persist keyword delivery mode and updater metadata"
        (is (= {:room-id "!project:example.org"
                :default-delivery-mode :steer
                :updated-at 2000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/set-room-default-delivery-mode! conn {:client-id "client-1"
                                                            :room-id "!project:example.org"
                                                            :default-delivery-mode "steer"
                                                            :updated-by-user "@alice:example.org"
                                                            :now-ms 2000})))
        (is (= :steer (store/room-default-delivery-mode @conn "!project:example.org")))
        (is (= {:room-id "!project:example.org"
                :default-delivery-mode :steer
                :updated-at 2000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/room-delivery-mode @conn "!project:example.org"))))
      (testing "leased slot rooms are also authorized"
        (is (= :reject
               (:default-delivery-mode
                (store/set-room-default-delivery-mode! conn {:client-id "client-2"
                                                             :room-id "!slot:example.org"
                                                             :default-delivery-mode :reject
                                                             :updated-by-user "@bob:example.org"
                                                             :now-ms 3000})))))
      (testing "unknown or unauthorized rooms are rejected"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Client is not registered for the target Matrix room"
             (store/set-room-default-delivery-mode! conn {:client-id "client-1"
                                                          :room-id "!slot:example.org"
                                                          :default-delivery-mode :follow-up
                                                          :updated-by-user "@alice:example.org"
                                                          :now-ms 4000})))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Unknown broker client"
             (store/set-room-default-delivery-mode! conn {:client-id "missing-client"
                                                          :room-id "!project:example.org"
                                                          :default-delivery-mode :follow-up
                                                          :updated-by-user "@alice:example.org"
                                                          :now-ms 4000}))))
      (testing "invalid delivery mode values are rejected before persistence"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Invalid room delivery mode"
             (store/set-room-default-delivery-mode! conn {:client-id "client-1"
                                                          :room-id "!project:example.org"
                                                          :default-delivery-mode "interrupt"
                                                          :updated-by-user "@alice:example.org"
                                                          :now-ms 5000})))
        (is (= :steer (store/room-default-delivery-mode @conn "!project:example.org")))))))

(deftest room-prompt-mode-is-persisted-and-authorized
  (with-conn
    (fn [conn]
      (store/register-client! conn {:now-ms 1000}
                              {:client/instance-id "client-1"
                               :protocol/version 1
                               :project {:project/id "project"}
                               :subscriptions {:rooms ["!project:example.org"]}})
      (testing "subscribed room prompt mode writes persist keyword mode and updater metadata"
        (is (= {:room-id "!project:example.org"
                :mode :commands-only
                :updated-at 2000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/set-room-prompt-mode! conn {:client-id "client-1"
                                                   :room-id "!project:example.org"
                                                   :mode "commands-only"
                                                   :updated-by-user "@alice:example.org"
                                                   :now-ms 2000})))
        (is (= {:room-id "!project:example.org"
                :mode :commands-only
                :updated-at 2000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/room-prompt-mode @conn "!project:example.org"))))
      (testing "mention aliases normalize to mentions"
        (is (= :mentions
               (:mode (store/set-room-prompt-mode! conn {:client-id "client-1"
                                                          :room-id "!project:example.org"
                                                          :mode "mention"
                                                          :updated-by-user "@alice:example.org"
                                                          :now-ms 3000})))))
      (testing "unknown or unauthorized rooms are rejected"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Client is not registered for the target Matrix room"
             (store/set-room-prompt-mode! conn {:client-id "client-1"
                                                 :room-id "!other:example.org"
                                                 :mode :all
                                                 :updated-by-user "@alice:example.org"
                                                 :now-ms 4000}))))
      (testing "invalid prompt mode values are rejected before persistence"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Invalid prompt mode"
             (store/set-room-prompt-mode! conn {:client-id "client-1"
                                                 :room-id "!project:example.org"
                                                 :mode "interrupt"
                                                 :updated-by-user "@alice:example.org"
                                                 :now-ms 5000})))
        (is (= :mentions (:mode (store/room-prompt-mode @conn "!project:example.org"))))))))

(deftest room-tool-message-settings-are-persisted-and-authorized
  (with-conn
    (fn [conn]
      (store/register-client! conn {:now-ms 1000}
                              {:client/instance-id "client-1"
                               :protocol/version 1
                               :project {:project/id "project"}
                               :subscriptions {:rooms ["!project:example.org"]}})
      (testing "missing settings return nil values"
        (is (= {:room-id "!project:example.org"
                :tool-messages-enabled? nil
                :tool-message-batch-ms nil
                :updated-at nil
                :updated-by-client nil
                :updated-by-user nil}
               (store/room-tool-message-settings @conn "!project:example.org"))))
      (testing "subscribed room writes persist tool message settings and updater metadata"
        (is (= {:room-id "!project:example.org"
                :tool-messages-enabled? false
                :tool-message-batch-ms 30000
                :updated-at 2000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/set-room-tool-message-settings! conn {:client-id "client-1"
                                                            :room-id "!project:example.org"
                                                            :tool-messages-enabled? false
                                                            :tool-message-batch-ms 30000
                                                            :updated-by-user "@alice:example.org"
                                                            :now-ms 2000})))
        (is (= {:room-id "!project:example.org"
                :tool-messages-enabled? false
                :tool-message-batch-ms 30000
                :updated-at 2000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/room-tool-message-settings @conn "!project:example.org"))))
      (testing "partial updates preserve existing values"
        (is (= {:room-id "!project:example.org"
                :tool-messages-enabled? true
                :tool-message-batch-ms 30000
                :updated-at 3000
                :updated-by-client "client-1"
                :updated-by-user "@alice:example.org"}
               (store/set-room-tool-message-settings! conn {:client-id "client-1"
                                                            :room-id "!project:example.org"
                                                            :tool-messages-enabled? true
                                                            :updated-by-user "@alice:example.org"
                                                            :now-ms 3000}))))
      (testing "unknown or unauthorized rooms are rejected"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Client is not registered for the target Matrix room"
             (store/set-room-tool-message-settings! conn {:client-id "client-1"
                                                          :room-id "!other:example.org"
                                                          :tool-messages-enabled? false
                                                          :now-ms 4000})))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Unknown broker client"
             (store/set-room-tool-message-settings! conn {:client-id "missing-client"
                                                          :room-id "!project:example.org"
                                                          :tool-messages-enabled? false
                                                          :now-ms 4000}))))
      (testing "invalid batch windows are rejected before persistence"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Invalid tool message batch window"
             (store/set-room-tool-message-settings! conn {:client-id "client-1"
                                                          :room-id "!project:example.org"
                                                          :tool-message-batch-ms 0
                                                          :now-ms 5000})))))))

(deftest idempotency-records-are-transactional-and-inline-capped
  (with-conn
    (fn [conn]
      (testing "request reservation owns the side effect exactly once"
        (is (= {:status :reserved}
               (select-keys (store/reserve-request!
                             conn
                             {:request-id "rid-1"
                              :operation :matrix/send-message
                              :fingerprint "same"
                              :owner-id #uuid "00000000-0000-0000-0000-000000000001"
                              :now-ms 1000
                              :retry-after-ms 1000})
                            [:status])))
        (is (= {:status :pending :retry-after-ms 1000}
               (select-keys (store/reserve-request!
                             conn
                             {:request-id "rid-1"
                              :operation :matrix/send-message
                              :fingerprint "same"
                              :owner-id #uuid "00000000-0000-0000-0000-000000000002"
                              :now-ms 1001
                              :retry-after-ms 1000})
                            [:status :retry-after-ms])))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Request id was reused with a different payload"
             (store/reserve-request!
              conn
              {:request-id "rid-1"
               :operation :matrix/send-message
               :fingerprint "different"
               :owner-id #uuid "00000000-0000-0000-0000-000000000003"
               :now-ms 1002}))))
      (testing "completed results are stored as small canonical JSON data only"
        (store/complete-request! conn {:request-id "rid-1" :now-ms 1100}
                                 {:room/id "!room:example.org" :event/id "$event:example.org"})
        (is (= {:status :completed
                :result {:room/id "!room:example.org" :event/id "$event:example.org"}}
               (select-keys (store/reserve-request!
                             conn
                             {:request-id "rid-1"
                              :operation :matrix/send-message
                              :fingerprint "same"
                              :owner-id #uuid "00000000-0000-0000-0000-000000000004"
                              :now-ms 1200})
                            [:status :result]))))
      (testing "inline idempotency results have a hard UTF-8 byte cap"
        (store/reserve-request! conn
                                {:request-id "rid-large"
                                 :operation :matrix/send-message
                                 :fingerprint "large"
                                 :owner-id #uuid "00000000-0000-0000-0000-000000000005"
                                 :now-ms 1300})
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Idempotency result JSON exceeds 2048 UTF-8 bytes"
             (store/complete-request! conn {:request-id "rid-large" :now-ms 1400}
                                      {:body (apply str (repeat 2050 "x"))})))))))
