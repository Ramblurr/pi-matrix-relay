(ns pi-matrix-relay.setup-test
  (:require [cljs.test :refer [async deftest is testing]]
            [pi-matrix-relay.setup :as setup]))

(deftest operators-editor-text-parses-mxids
  (is (= ["@alice:example.org" "@bob:example.org" "@carol:example.org"]
         (setup/parse-operators "@alice:example.org, @bob:example.org\n@carol:example.org"))))

(deftest config-from-fields-keeps-password-in-global-config-for-current-adapter
  (is (= {:matrix {:homeserver-url "https://matrix.example.org"
                   :user-id "@bot:example.org"
                   :password "secret"
                   :operators ["@alice:example.org"]
                   :encrypted? true
                   :device-name "pi-matrix-relay-broker"}}
         (setup/config-from-fields {:homeserver-url "https://matrix.example.org"
                                    :user-id "@bot:example.org"
                                    :password "secret"
                                    :operators ["@alice:example.org"]
                                    :encrypted? true}))))

(deftest run-setup-writes-config-installs-service-and-checks-health
  (async done
    (let [calls* (atom [])
          input-values (atom ["https://matrix.example.org"
                              "@bot:example.org"
                              "secret"])
          confirm-values (atom [true true])
          deps {:input! (fn [label placeholder]
                          (swap! calls* conj [:input label placeholder])
                          (js/Promise.resolve (let [v (first @input-values)]
                                                (swap! input-values subvec 1)
                                                v)))
                :editor! (fn [label initial]
                           (swap! calls* conj [:editor label initial])
                           (js/Promise.resolve "@alice:example.org\n@bob:example.org"))
                :confirm! (fn [title message]
                            (swap! calls* conj [:confirm title message])
                            (js/Promise.resolve (let [v (first @confirm-values)]
                                                  (swap! confirm-values subvec 1)
                                                  v)))
                :write-global-config! (fn [config]
                                        (swap! calls* conj [:write-global-config config]))
                :install-service! (fn []
                                    (swap! calls* conj [:install-service])
                                    (js/Promise.resolve nil))
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :notify! (fn [message level]
                           (swap! calls* conj [:notify message level]))
                :set-status! (fn [status]
                               (swap! calls* conj [:status status]))}]
      (-> (setup/run-setup! deps)
          (.then (fn [result]
                   (is (= {:matrix {:connected true :userId "@bot:example.org"}}
                          result))
                   (is (some #(= [:install-service] %) @calls*))
                   (is (some #(= [:health] %) @calls*))
                   (is (some (fn [[op config]]
                               (and (= :write-global-config op)
                                    (= "secret" (get-in config [:matrix :password]))
                                    (= ["@alice:example.org" "@bob:example.org"]
                                       (get-in config [:matrix :operators]))))
                             @calls*))
                   (is (some #(= [:status "matrix: @bot:example.org"] %) @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))
