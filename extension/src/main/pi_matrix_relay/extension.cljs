(ns pi-matrix-relay.extension)

(defn greeting [name]
  (str "Hello, " name ", from ClojureScript!"))

(defn hello-handler [args ^js ctx]
  (let [target (if (and (string? args)
                        (not= "" (.trim args)))
                 (.trim args)
                 "world")
        ^js ui (.-ui ctx)]
    (.notify ui (greeting target) "info")))

(defn init [^js pi]
  (.registerCommand pi "hello"
    #js {:description "Say hello from the ClojureScript Pi extension"
         :handler hello-handler}))
