(ns pi-matrix-relay.broker.wire-format-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [pi-matrix-relay.broker.api :as api]
            [pi-matrix-relay.broker.db :as db]
            [pi-matrix-relay.broker.events :as events]
            [pi-matrix-relay.broker.store :as store]
            [pi-matrix-relay.broker.test-util :as tu]))

(defn edn-response
  [response]
  (when-let [body (:body response)]
    (edn/read-string (if (string? body)
                       body
                       (slurp body)))))

(defn edn-request
  ([app method uri]
   (edn-request app method uri nil))
  ([app method uri body]
   (let [[path query-string] (str/split uri #"\?" 2)
         body-stream (when (some? body)
                       (java.io.ByteArrayInputStream.
                        (.getBytes (pr-str body) "UTF-8")))]
     (app (cond-> {:request-method method
                   :uri path
                   :headers (cond-> {"accept" "application/edn"}
                              body-stream (assoc "content-type" "application/edn"))}
            query-string (assoc :query-string query-string)
            body-stream (assoc :body body-stream))))))

(defn edn-call
  [app method uri body]
  (edn-response (edn-request app method uri body)))

(defn with-env
  [f]
  (let [conn (tu/test-db-conn)
        env (tu/test-env (tu/fake-gateway) conn)]
    (try
      (f env conn)
      (finally
        (db/release-conn! conn)))))

(deftest api-speaks-edn-with-domain-keys
  (testing "broker HTTP boundary accepts and returns application/edn without camelCase translation"
    (with-env
      (fn [env conn]
        (let [app (api/app env)
              response (edn-request app :post "/v1/clients"
                                    {:request/id "register-edn"
                                     :client/instance-id "instance-1"
                                     :protocol/version 1
                                     :project {:project/id "project"
                                               :project/root "/work/project"}
                                     :subscriptions {:rooms ["!project:example.org"]}})]
          (is (str/starts-with? (get-in response [:headers "Content-Type"])
                                "application/edn"))
          (is (= {:ok true
                  :data {:client/id "instance-1"
                         :event-stream/path "/v1/clients/instance-1/events"
                         :heartbeat/seconds 30
                         :matrix/global-operators ["@operator:example.org"]}}
                 (edn-response response)))
          (is (= "instance-1" (:client-id (store/client @conn "instance-1")))))))))

(deftest sse-data-uses-edn-domain-keys
  (testing "SSE frames preserve namespaced EDN event data"
    (let [payload (events/format-sse {:id "evt-1"
                                      :event "matrix.message"
                                      :data {:type "matrix.message"
                                             :room/id "!room:example.org"
                                             :event/id "$event:example.org"}})
          data-line (some #(when (str/starts-with? % "data: ")
                             (subs % 6))
                          (str/split-lines payload))]
      (is (not (str/includes? payload "{\"")))
      (is (= {:type "matrix.message"
              :room/id "!room:example.org"
              :event/id "$event:example.org"}
             (edn/read-string data-line))))))
