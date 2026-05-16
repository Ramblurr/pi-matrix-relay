(ns pi-matrix-relay.extension-test
  (:require [cljs.test :refer [async deftest is testing]]
            [pi-matrix-relay.extension :as extension]))

(deftest greeting-includes-target
  (testing "the extension test runner can load project namespaces"
    (is (= "Hello, Matrix, from ClojureScript!"
           (extension/greeting "Matrix")))))

(deftest registers-long-and-short-commands-and-send-tool
  (let [commands* (atom {})
        tools* (atom {})
        pi #js {:registerCommand (fn [name opts]
                                   (swap! commands* assoc name opts))
                :registerTool (fn [tool]
                                (swap! tools* assoc (.-name tool) tool))}]
    (extension/init pi)
    (is (= #{"matrix-relay" "mr"}
           (set (keys @commands*))))
    (is (= #{"send_matrix_message"}
           (set (keys @tools*))))
    (is (fn? (.-execute ^js (get @tools* "send_matrix_message"))))))

(deftest room-bind-resolves-room-and-writes-project-config
  (async done
    (let [notifications* (atom [])
          written* (atom nil)
          deps {:resolve-room! (fn [room]
                                 (js/Promise.resolve {:roomId "!room:example.org"
                                                      :canonicalAlias room
                                                      :name "Pi Room"}))
                :read-project-config! (fn [_cwd] {})
                :write-project-config! (fn [cwd config]
                                        (reset! written* {:cwd cwd :config config}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "room bind #pi:example.org ops" ctx)
          (.then (fn [_]
                   (is (= {:cwd "/work/project"
                           :config {:rooms {"ops" {:alias "ops"
                                                   :roomId "!room:example.org"
                                                   :canonicalAlias "#pi:example.org"
                                                   :name "Pi Room"
                                                   :mode "mentions"
                                                   :busy "follow-up"}}}}
                          @written*))
                   (is (= [["Bound ops to !room:example.org" "info"]]
                          @notifications*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest setup-command-delegates-to-setup-runner-with-ui-functions
  (async done
    (let [deps-seen* (atom nil)
          deps {:run-setup! (fn [setup-deps]
                              (reset! deps-seen* setup-deps)
                              (js/Promise.resolve {:matrix {:connected true}}))}
          ctx #js {:ui #js {:input (fn [_ _] (js/Promise.resolve "input"))
                            :editor (fn [_ _] (js/Promise.resolve "editor"))
                            :confirm (fn [_ _] (js/Promise.resolve true))
                            :notify (fn [_ _])
                            :setStatus (fn [_ _])}}]
      (-> (extension/handle-command! deps "setup" ctx)
          (.then (fn [_]
                   (is (fn? (:input! @deps-seen*)))
                   (is (fn? (:editor! @deps-seen*)))
                   (is (fn? (:confirm! @deps-seen*)))
                   (is (fn? (:notify! @deps-seen*)))
                   (is (fn? (:set-status! @deps-seen*)))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-matrix-message-tool-reuses-bound-target-resolution
  (async done
    (let [sent* (atom nil)
          deps {:read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :send-message! (fn [room-id message]
                                (reset! sent* {:room-id room-id :message message})
                                (js/Promise.resolve {:eventId "$event:example.org"}))}
          ctx #js {:cwd "/work/project"}]
      (-> (extension/execute-send-matrix-message! deps {:target "ops"
                                                        :message "tool says hello"}
                                                ctx)
          (.then (fn [result]
                   (is (= {:room-id "!room:example.org" :message "tool says hello"}
                          @sent*))
                   (is (= {:content [{:type "text"
                                      :text "Sent Matrix message $event:example.org to !room:example.org"}]
                           :details {:roomId "!room:example.org"
                                     :eventId "$event:example.org"
                                     :target "ops"}}
                          result))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))

(deftest send-command-resolves-bound-target-and-posts-message
  (async done
    (let [sent* (atom nil)
          notifications* (atom [])
          deps {:read-project-config! (fn [_cwd]
                                        {:rooms {"ops" {:alias "ops"
                                                        :roomId "!room:example.org"}}})
                :send-message! (fn [room-id message]
                                (reset! sent* {:room-id room-id :message message})
                                (js/Promise.resolve {:eventId "$event:example.org"}))}
          ctx #js {:cwd "/work/project"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications* conj [message level]))}}]
      (-> (extension/handle-command! deps "send ops hello from pi" ctx)
          (.then (fn [_]
                   (is (= {:room-id "!room:example.org" :message "hello from pi"}
                          @sent*))
                   (is (= [["Sent Matrix message $event:example.org" "info"]]
                          @notifications*))
                   (done)))
          (.catch (fn [err]
                    (is false (.-stack err))
                    (done)))))))
