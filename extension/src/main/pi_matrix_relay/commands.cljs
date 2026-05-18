(ns pi-matrix-relay.commands
  (:require [clojure.string :as str]))

(defn- error
  [message]
  {:op :error
   :message message})

(defn parse
  "Parse `/matrix-relay` or `/mr` command arguments into data.

  The Pi command name is stripped before this function is called. Message bodies
  for `send` preserve all spaces after the target argument."
  [args]
  (let [args (str/trim (or args ""))]
    (cond
      (str/blank? args)
      {:op :help}

      (= "setup" args)
      {:op :setup}

      (= "help" args)
      {:op :help}

      (= "status" args)
      {:op :status}

      (#{"connect" "disconnect" "reconnect"} args)
      {:op :control
       :action args}

      :else
      (if-let [[_ request-id] (re-matches #"__new-session\s+(\S+)" args)]
        {:op :internal-new-session
         :request-id request-id}
        (if-let [[_ room alias] (re-matches #"room\s+bind\s+(\S+)(?:\s+(\S+))?" args)]
          (cond-> {:op :room-bind
                   :room room}
            alias (assoc :alias alias))
          (if-let [[_ target message] (re-matches #"send\s+(\S+)\s+([\s\S]+)" args)]
            {:op :send
             :target target
             :message message}
            (error (str "Unknown matrix-relay command: " args))))))))
