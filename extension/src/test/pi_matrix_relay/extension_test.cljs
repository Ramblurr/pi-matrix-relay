(ns pi-matrix-relay.extension-test
  (:require [cljs.test :refer [async deftest is testing]]
            [pi-matrix-relay.extension :as extension]))

(deftest greeting-includes-target
  (testing "the extension test runner can load project namespaces"
    (is (= "Hello, Matrix, from ClojureScript!"
           (extension/greeting "Matrix")))))

(deftest registers-long-and-short-commands
  (let [commands* (atom {})
        pi #js {:registerCommand (fn [name opts]
                                   (swap! commands* assoc name opts))}]
    (extension/init pi)
    (is (= #{"matrix-relay" "mr"}
           (set (keys @commands*))))))

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
