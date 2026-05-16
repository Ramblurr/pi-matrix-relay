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

(deftest setup-field-validation-rejects-bad-urls-and-mxids
  (testing "homeserver must be a real https URL"
    (try
      (setup/validate-fields! {:homeserver-url "matrix.example.org"
                               :user-id "@bot:example.org"
                               :password "secret"
                               :operators []
                               :encrypted? true})
      (is false "expected validation error")
      (catch js/Error e
        (is (= "Matrix homeserver URL must be an https:// URL."
               (.-message e))))))
  (testing "bot user id and operators must be Matrix IDs"
    (try
      (setup/validate-fields! {:homeserver-url "https://matrix.example.org"
                               :user-id "bot"
                               :password "secret"
                               :operators ["@alice:example.org"]
                               :encrypted? true})
      (is false "expected validation error")
      (catch js/Error e
        (is (= "Matrix bot user ID must look like @user:server."
               (.-message e)))))
    (try
      (setup/validate-fields! {:homeserver-url "https://matrix.example.org"
                               :user-id "@bot:example.org"
                               :password "secret"
                               :operators ["alice"]
                               :encrypted? true})
      (is false "expected validation error")
      (catch js/Error e
        (is (= "Operator MXID must look like @user:server: alice"
               (.-message e)))))))

(deftest run-setup-reprompts-invalid-homeserver-before-continuing
  (async done
    (let [calls* (atom [])
          homeserver-values (atom ["matrix.example.org" "https://matrix.example.org"])
          deps {:read-global-config! (constantly {})
                :editor! (fn [label initial]
                           (swap! calls* conj [:editor label initial])
                           (js/Promise.resolve
                            (case label
                              "Matrix homeserver URL" (let [v (first @homeserver-values)]
                                                        (swap! homeserver-values subvec 1)
                                                        v)
                              "Matrix bot user ID" "@bot:example.org"
                              "Global operator MXIDs, one per line" "@alice:example.org")))
                :input! (fn [label placeholder]
                          (swap! calls* conj [:input label placeholder])
                          (js/Promise.resolve "secret"))
                :confirm! (fn [title message]
                            (swap! calls* conj [:confirm title message])
                            (js/Promise.resolve false))
                :write-global-config! (fn [config]
                                        (swap! calls* conj [:write-global-config config]))
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :notify! (fn [message level]
                           (swap! calls* conj [:notify message level]))
                :set-status! (fn [status]
                               (swap! calls* conj [:status status]))}]
      (-> (setup/run-setup! deps)
          (.then (fn [_]
                   (is (= [[:editor "Matrix homeserver URL" "https://matrix.example.org"]
                           [:notify "Matrix homeserver URL must be an https:// URL." "error"]
                           [:editor "Matrix homeserver URL" "matrix.example.org"]
                           [:editor "Matrix bot user ID" "@pi:example.org"]]
                          (take 4 @calls*)))
                   (is (some (fn [[op config]]
                               (and (= :write-global-config op)
                                    (= "https://matrix.example.org"
                                       (get-in config [:matrix :homeserver-url]))))
                             @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest run-setup-reprompts-invalid-operators-before-encryption-confirm
  (async done
    (let [calls* (atom [])
          operator-values (atom ["alice" "@alice:example.org"])
          deps {:read-global-config! (constantly {})
                :editor! (fn [label initial]
                           (swap! calls* conj [:editor label initial])
                           (js/Promise.resolve
                            (case label
                              "Matrix homeserver URL" "https://matrix.example.org"
                              "Matrix bot user ID" "@bot:example.org"
                              "Global operator MXIDs, one per line" (let [v (first @operator-values)]
                                                                      (swap! operator-values subvec 1)
                                                                      v))))
                :input! (fn [label placeholder]
                          (swap! calls* conj [:input label placeholder])
                          (js/Promise.resolve "secret"))
                :confirm! (fn [title message]
                            (swap! calls* conj [:confirm title message])
                            (js/Promise.resolve false))
                :write-global-config! (fn [config]
                                        (swap! calls* conj [:write-global-config config]))
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :notify! (fn [message level]
                           (swap! calls* conj [:notify message level]))
                :set-status! (fn [status]
                               (swap! calls* conj [:status status]))}]
      (-> (setup/run-setup! deps)
          (.then (fn [_]
                   (let [events @calls*
                         operator-start (first (keep-indexed
                                                (fn [idx call]
                                                  (when (= [:editor "Global operator MXIDs, one per line" ""] call)
                                                    idx))
                                                events))
                         after-operator (take 4 (drop operator-start events))]
                     (is (= [[:editor "Global operator MXIDs, one per line" ""]
                             [:notify "Operator MXID must look like @user:server: alice" "error"]
                             [:editor "Global operator MXIDs, one per line" "alice"]]
                            (take 3 after-operator)))
                     (is (= :confirm (ffirst (drop 3 after-operator)))))
                   (is (some (fn [[op config]]
                               (and (= :write-global-config op)
                                    (= ["@alice:example.org"]
                                       (get-in config [:matrix :operators]))))
                             @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest run-setup-prefills-existing-config-and-keeps-existing-password-on-blank
  (async done
    (let [calls* (atom [])
          deps {:read-global-config! (fn []
                                       {:matrix {:homeserver-url "https://matrix.example.org"
                                                 :user-id "@bot:example.org"
                                                 :password "old-secret"
                                                 :operators ["@alice:example.org" "@bob:example.org"]
                                                 :encrypted? true}})
                :input! (fn [label placeholder]
                          (swap! calls* conj [:input label placeholder])
                          (js/Promise.resolve ""))
                :editor! (fn [label initial]
                           (swap! calls* conj [:editor label initial])
                           (js/Promise.resolve
                            (case label
                              "Matrix homeserver URL" ""
                              "Matrix bot user ID" ""
                              initial)))
                :confirm! (fn [title message]
                            (swap! calls* conj [:confirm title message])
                            (js/Promise.resolve false))
                :write-global-config! (fn [config]
                                        (swap! calls* conj [:write-global-config config]))
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :notify! (fn [message level]
                           (swap! calls* conj [:notify message level]))
                :set-status! (fn [status]
                               (swap! calls* conj [:status status]))}]
      (-> (setup/run-setup! deps)
          (.then (fn [_]
                   (is (some #(= [:editor "Matrix homeserver URL" "https://matrix.example.org"] %) @calls*))
                   (is (some #(= [:editor "Matrix bot user ID" "@bot:example.org"] %) @calls*))
                   (is (some #(= [:input "Matrix bot password (leave blank to keep existing password)" ""] %) @calls*))
                   (is (some #(= [:editor "Global operator MXIDs, one per line" "@alice:example.org\n@bob:example.org"] %) @calls*))
                   (is (not-any? #(= [:input "Matrix homeserver URL" "https://matrix.example.org"] %) @calls*))
                   (is (not-any? #(= [:input "Matrix bot user ID" "@bot:example.org"] %) @calls*))
                   (is (some (fn [[op config]]
                               (and (= :write-global-config op)
                                    (= "old-secret" (get-in config [:matrix :password]))
                                    (= "https://matrix.example.org" (get-in config [:matrix :homeserver-url]))
                                    (= ["@alice:example.org" "@bob:example.org"]
                                       (get-in config [:matrix :operators]))))
                             @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest run-setup-normalizes-legacy-schemeless-existing-homeserver
  (async done
    (let [calls* (atom [])
          deps {:read-global-config! (fn []
                                       {:matrix {:homeserver-url "matrix.example.org"
                                                 :user-id "@bot:example.org"
                                                 :password "old-secret"
                                                 :operators ["@alice:example.org"]
                                                 :encrypted? true}})
                :input! (fn [label placeholder]
                          (swap! calls* conj [:input label placeholder])
                          (js/Promise.resolve ""))
                :editor! (fn [label initial]
                           (swap! calls* conj [:editor label initial])
                           (js/Promise.resolve initial))
                :confirm! (fn [title message]
                            (swap! calls* conj [:confirm title message])
                            (js/Promise.resolve false))
                :write-global-config! (fn [config]
                                        (swap! calls* conj [:write-global-config config]))
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve {:matrix {:connected true
                                                         :userId "@bot:example.org"}}))
                :notify! (fn [message level]
                           (swap! calls* conj [:notify message level]))
                :set-status! (fn [status]
                               (swap! calls* conj [:status status]))}]
      (-> (setup/run-setup! deps)
          (.then (fn [_]
                   (is (some #(= [:editor "Matrix homeserver URL" "https://matrix.example.org"] %) @calls*))
                   (is (some (fn [[op config]]
                               (and (= :write-global-config op)
                                    (= "https://matrix.example.org"
                                       (get-in config [:matrix :homeserver-url]))))
                             @calls*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest run-setup-retries-health-until-matrix-connects
  (async done
    (let [calls* (atom [])
          health-values (atom [{:matrix {:connected false}}
                               {:matrix {:connected true :userId "@bot:example.org"}}])
          deps {:input! (fn [_ _] (js/Promise.resolve ""))
                :editor! (fn [_ initial] (js/Promise.resolve initial))
                :confirm! (fn [_ _] (js/Promise.resolve false))
                :read-global-config! (constantly {:matrix {:homeserver-url "https://matrix.example.org"
                                                           :user-id "@bot:example.org"
                                                           :password "secret"
                                                           :operators []
                                                           :encrypted? true}})
                :write-global-config! (fn [_])
                :sleep! (fn [ms]
                          (swap! calls* conj [:sleep ms])
                          (js/Promise.resolve nil))
                :health-attempts 2
                :health-delay-ms 5
                :health! (fn []
                           (swap! calls* conj [:health])
                           (js/Promise.resolve (let [v (first @health-values)]
                                                 (swap! health-values subvec 1)
                                                 v)))
                :notify! (fn [_ _])
                :set-status! (fn [_])}]
      (-> (setup/run-setup! deps)
          (.then (fn [result]
                   (is (= {:matrix {:connected true :userId "@bot:example.org"}}
                          result))
                   (is (= [[:health] [:sleep 5] [:health]]
                          (filter #(#{:health :sleep} (first %)) @calls*)))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest run-setup-writes-config-installs-service-and-checks-health
  (async done
    (let [calls* (atom [])
          confirm-values (atom [true true])
          deps {:read-global-config! (constantly {})
                :input! (fn [label placeholder]
                          (swap! calls* conj [:input label placeholder])
                          (js/Promise.resolve "secret"))
                :editor! (fn [label initial]
                           (swap! calls* conj [:editor label initial])
                           (js/Promise.resolve
                            (case label
                              "Matrix homeserver URL" "https://matrix.example.org"
                              "Matrix bot user ID" "@bot:example.org"
                              "@alice:example.org\n@bob:example.org")))
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
                   (is (some #(= [:input "Matrix bot password" ""] %) @calls*))
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
