(ns pi-matrix-relay.broker.system-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [donut.system :as ds]
            [pi-matrix-relay.broker.json :as json]
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
          running (system/start! {:paths {:runtime-dir "/tmp/pi-matrix-relay-system-test"
                                          :socket-path "/tmp/pi-matrix-relay-system-test/broker.sock"}
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

(deftest broker-system-can-bind-a-unix-domain-socket
  (testing "the default local transport creates the configured broker socket"
    (let [socket-path "/tmp/pi-matrix-relay-system-test-uds/broker.sock"
          running (system/start! {:paths {:runtime-dir "/tmp/pi-matrix-relay-system-test-uds"
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
