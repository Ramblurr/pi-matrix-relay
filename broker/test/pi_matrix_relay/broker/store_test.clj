(ns pi-matrix-relay.broker.store-test
  (:require [clojure.test :refer [deftest is testing]]
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

(deftest clients-subscriptions-and-room-authorization-live-in-datahike
  (with-conn
    (fn [conn]
      (testing "client registration persists subscriptions and uses instanceId as clientId"
        (let [registration (store/register-client!
                            conn
                            {:now-ms 1000
                             :heartbeat-seconds 30
                             :global-operators ["@operator:example.org"]}
                            {:instanceId "instance-1"
                             :protocolVersion 1
                             :project {:id "project" :root "/work/project" :displayName "Project"}
                             :metadata {:piSessionName "main"}
                             :subscriptions {:rooms ["!project:example.org"]}})]
          (is (= {:client-id "instance-1"
                  :heartbeat-seconds 30
                  :global-operators ["@operator:example.org"]}
                 (select-keys registration [:client-id :heartbeat-seconds :global-operators])))
          (is (= {:project-room true
                  :unknown false}
                 {:project-room (store/known-room-for-client? @conn "instance-1" "!project:example.org")
                  :unknown (store/known-room-for-client? @conn "instance-1" "!other:example.org")}))))
      (testing "subscription replacement removes rooms without touching lease-derived authorization"
        (store/update-subscriptions! conn "instance-1" ["!other:example.org"])
        (is (= {:old false :new true}
               {:old (store/known-room-for-client? @conn "instance-1" "!project:example.org")
                :new (store/known-room-for-client? @conn "instance-1" "!other:example.org")}))))))

(deftest slot-reservations-use-transaction-time-state-and-cas-lifecycle
  (with-conn
    (fn [conn]
      (doseq [client-id ["client-1" "client-2"]]
        (store/register-client! conn {:now-ms 1000}
                                {:instanceId client-id
                                 :protocolVersion 1
                                 :project {:id "project"}}))
      (testing "two reservations made before Matrix room creation occupy A then B"
        (let [reservation-1 (store/reserve-slot! conn {:now-ms 1100}
                                                 {:client-id "client-1"
                                                  :project {:id "project"}})
              reservation-2 (store/reserve-slot! conn {:now-ms 1200}
                                                 {:client-id "client-2"
                                                  :project {:id "project"}})]
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
            (is (= [{:slot "A" :state :leased :room-id "!project-A:example.org"}
                    {:slot "B" :state :leased :room-id "!project-B:example.org"}]
                   (mapv #(select-keys % [:slot :state :room-id]) [lease-1 lease-2]))))))
      (testing "release uses a state CAS and frees the slot for reuse"
        (is (= {:released true}
               (store/release-slot! conn {:now-ms 1500 :client-id "client-1" :slot "A"})))
        (let [reservation-3 (store/reserve-slot! conn {:now-ms 1600}
                                                 {:client-id "client-1"
                                                  :project {:id "project"}})]
          (is (= "A" (:slot reservation-3))))))))

(deftest stale-and-suspect-lease-transitions-are-cas-backed
  (with-conn
    (fn [conn]
      (doseq [client-id ["client-1" "client-2"]]
        (store/register-client! conn {:now-ms 1000}
                                {:instanceId client-id
                                 :protocolVersion 1
                                 :project {:id "project"}}))
      (let [r1 (store/reserve-slot! conn {:now-ms 1000} {:client-id "client-1" :project {:id "project"}})
            r2 (store/reserve-slot! conn {:now-ms 1000} {:client-id "client-2" :project {:id "project"}})]
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
                                 {:roomId "!room:example.org" :eventId "$event:example.org"})
        (is (= {:status :completed
                :result {:roomId "!room:example.org" :eventId "$event:example.org"}}
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
