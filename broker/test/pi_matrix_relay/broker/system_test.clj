(ns pi-matrix-relay.broker.system-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [donut.system :as ds]
            [pi-matrix-relay.broker.json :as json]
            [pi-matrix-relay.broker.state :as state]
            [pi-matrix-relay.broker.system :as system]
            [pi-matrix-relay.broker.test-util :as tu])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]))

(defn http-get
  [url]
  (let [client (HttpClient/newHttpClient)
        request (-> (HttpRequest/newBuilder (URI/create url))
                    (.GET)
                    (.build))]
    (.send client request (HttpResponse$BodyHandlers/ofString))))

(deftest broker-system-starts-http-server-through-donut
  (testing "donut lifecycle starts the fake Matrix gateway, Ring app, and http-kit server"
    (let [gateway (tu/fake-gateway)
          runtime-dir (str "/tmp/pi-matrix-relay-system-test-" (random-uuid))
          running (system/start! {:paths {:runtime-dir runtime-dir
                                          :state-dir (str runtime-dir "/state")
                                          :socket-path (str runtime-dir "/broker.sock")}
                                  :http {:transport :tcp :port 0}
                                  :matrix-gateway gateway})]
      (try
        (let [http-server (ds/instance running [:broker :http-server])
              response (http-get (str "http://127.0.0.1:" (:port http-server) "/v1/health"))]
          (is (= {:server {:transport :tcp
                           :socket-path nil
                           :port-positive? true}
                  :health {:ok true
                           :data {:status "ok"
                                  :matrix {:connected true
                                           :userId "@bot:example.org"
                                           :encrypted true}}}}
                 {:server {:transport (:transport http-server)
                           :socket-path (:socket-path http-server)
                           :port-positive? (pos? (:port http-server))}
                  :health (json/read-json (.body response))})))
        (finally
          (system/stop! running))))))

(deftest broker-system-refuses-to-start-twice-for-the-same-runtime-dir
  (testing "a process lock prevents a second broker from stealing the Unix socket"
    (let [runtime-dir (str "/tmp/pi-matrix-relay-system-test-lock-" (random-uuid))
          socket-path (str runtime-dir "/broker.sock")
          running (system/start! {:paths {:runtime-dir runtime-dir
                                          :state-dir (str runtime-dir "/state")
                                          :socket-path socket-path}
                                  :http {:transport :tcp :port 0}
                                  :matrix-gateway (tu/fake-gateway)})]
      (try
        (let [ex (try
                   (system/start! {:paths {:runtime-dir runtime-dir
                                           :state-dir (str runtime-dir "/state")
                                           :socket-path socket-path}
                                   :http {:transport :tcp :port 0}
                                   :matrix-gateway (tu/fake-gateway)})
                   nil
                   (catch clojure.lang.ExceptionInfo ex
                     ex))]
          (is (= {:code :broker_already_running
                  :lock-path (str runtime-dir "/broker.lock")}
                 (select-keys (ex-data ex) [:code :lock-path]))))
        (finally
          (system/stop! running))))))

(deftest broker-system-can-bind-a-unix-domain-socket
  (testing "the default local transport creates the configured broker socket"
    (let [runtime-dir (str "/tmp/pi-matrix-relay-system-test-uds-" (random-uuid))
          socket-path (str runtime-dir "/broker.sock")
          running (system/start! {:paths {:runtime-dir runtime-dir
                                          :state-dir (str runtime-dir "/state")
                                          :socket-path socket-path}
                                  :matrix-gateway (tu/fake-gateway)})]
      (try
        (is (= {:transport :uds
                :socket-path socket-path
                :socket-exists? true}
               {:transport (:transport (ds/instance running [:broker :http-server]))
                :socket-path (:socket-path (ds/instance running [:broker :http-server]))
                :socket-exists? (.exists (io/file socket-path))}))
        (finally
          (system/stop! running))))))

(deftest stale-lease-sweep-sends-one-minimal-operational-notice
  (testing "the broker releases stale leases without pretending the Pi session ended cleanly"
    (let [gateway (tu/fake-gateway)
          state* (atom (state/empty-state))]
      (state/register-client!
       state*
       {:client-id-fn (constantly "client-stale")
        :now 1000
        :heartbeat-seconds 30
        :global-operators []}
       {:clientInstanceId "instance-stale"
        :protocolVersion 1
        :project {:id "project"}})
      (state/acquire-slot!
       state*
       {:now 1000}
       {:client-id "client-stale"
        :project {:id "project"}
        :room-id "!slot-stale:example.org"
        :room-name "project-A"})
      (let [stale (system/sweep-stale-leases! {:state* state*
                                               :matrix-gateway gateway
                                               :config {:leases {:heartbeat-seconds 30
                                                                 :stale-after-missed 3}}
                                               :now 100000})
            send-request (second (first (filter #(= :send-message (first %))
                                                (tu/calls gateway))))]
        (is (= {:stale-slots ["A"]
                :slot-state "released"
                :send-target {:roomId "!slot-stale:example.org"}
                :send-count 1}
               {:stale-slots (mapv :slot stale)
                :slot-state (get-in (state/list-slots @state* "project") [:slots 0 :state])
                :send-target (:target send-request)
                :send-count (count (filter #(= :send-message (first %))
                                           (tu/calls gateway)))}))
        (is (re-find #"slot A client disconnected unexpectedly" (:body send-request)))
        (is (re-find #"Last heartbeat: 1970-01-01T00:00:01Z" (:body send-request)))
        (is (not (re-find #"ended cleanly|tombstone|session ended" (:body send-request))))))))
