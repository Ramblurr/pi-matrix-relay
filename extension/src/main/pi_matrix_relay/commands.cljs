(ns pi-matrix-relay.commands
  (:require [clojure.string :as str]
            [pi-matrix-relay.config :as config]))

(defn- error
  [message]
  {:op :error
   :message message})

(defn- parse-room-bind
  [bind-args]
  (let [[room & rest] (str/split (str/trim bind-args) #"\s+")]
    (cond
      (str/blank? room)
      (error "Usage: room bind <room> [alias] [mode]")

      (empty? rest)
      {:op :room-bind
       :room room}

      (and (= 1 (count rest)) (config/valid-room-prompt-mode? (first rest)))
      {:op :room-bind
       :room room
       :mode (config/normalize-prompt-mode (first rest))}

      (= 1 (count rest))
      {:op :room-bind
       :room room
       :alias (first rest)}

      (= 2 (count rest))
      {:op :room-bind
       :room room
       :alias (first rest)
       :mode (config/normalize-prompt-mode (second rest))}

      :else
      (error "Usage: room bind <room> [alias] [mode]"))))

(defn- parse-verify
  [verify-args]
  (let [parts (str/split (str/trim verify-args) #"\s+")
        [action a b & more] parts]
    (cond
      (or (str/blank? verify-args) (str/blank? action))
      (error "Usage: verify <bootstrap|start|accept|start-sas|confirm|no-match|cancel|status> ...")

      (= "status" action)
      (if (or a (seq more))
        (error "Usage: verify status")
        {:op :verify-status})

      (= "bootstrap" action)
      (if (or a (seq more))
        (error "Usage: verify bootstrap")
        {:op :verify-bootstrap})

      (= "start" action)
      (cond
        (or (str/blank? a) (seq more))
        (error "Usage: verify start <user-id> [device-id]")

        (str/blank? b)
        {:op :verify-start
         :user/id a}

        :else
        {:op :verify-start
         :user/id a
         :device/id b})

      (#{"accept" "start-sas" "confirm" "no-match" "cancel"} action)
      (if (or (str/blank? a) b (seq more))
        (error (str "Usage: verify " action " <verification-id>"))
        {:op (keyword (str "verify-" action))
         :verification/id a})

      :else
      (error (str "Unknown verify action: " action)))))

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
        (if-let [[_ verify-args] (re-matches #"verify\s+([\s\S]+)" args)]
          (parse-verify verify-args)
        (if-let [[_ bind-args] (re-matches #"room\s+bind\s+([\s\S]+)" args)]
          (parse-room-bind bind-args)
          (if-let [[_ target mode] (re-matches #"room\s+mode\s+(\S+)\s+(\S+)" args)]
            {:op :room-prompt-mode
             :target target
             :mode (config/normalize-prompt-mode mode)}
            (if-let [[_ verbosity] (re-matches #"progress\s+(\S+)" args)]
              (if (config/valid-progress-verbosity? verbosity)
                {:op :progress-verbosity
                 :verbosity (config/normalize-progress-verbosity verbosity)}
                (error "Usage: progress <quiet|normal|verbose>"))
              (if-let [[_ target message] (re-matches #"send\s+(\S+)\s+([\s\S]+)" args)]
                {:op :send
                 :target target
                 :message message}
                (error (str "Unknown matrix-relay command: " args)))))))))))
