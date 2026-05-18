(ns pi-matrix-relay.broker.matrix
  (:require [clojure.string :as str]))

(defprotocol MatrixGateway
  (start! [gateway])
  (stop! [gateway])
  (health [gateway])
  (list-rooms! [gateway])
  (resolve-room! [gateway room])
  (create-room! [gateway request])
  (ensure-users-power-level! [gateway request])
  (leave-room! [gateway request])
  (send-message! [gateway request])
  (set-typing! [gateway request])
  (send-reaction! [gateway request])
  (send-file! [gateway request])
  (download-media! [gateway request])
  (transcribe-media! [gateway request])
  (verification-start! [gateway request])
  (verification-confirm! [gateway verification-id])
  (verification-cancel! [gateway verification-id])
  (verification-status [gateway]))

(defn ex
  [code message details]
  (ex-info message (merge {:code code} details)))

(defn unavailable
  [code message details]
  (throw (ex code message details)))

(defrecord DisabledGateway [reason]
  MatrixGateway
  (start! [this] this)
  (stop! [_] nil)
  (health [_]
    {:status "degraded"
     :matrix/connected? false
     :matrix/encrypted? true
     :reason reason})
  (list-rooms! [_]
    (unavailable :matrix_not_configured "Matrix client is not configured." {}))
  (resolve-room! [_ room]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:room room}))
  (create-room! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (ensure-users-power-level! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (leave-room! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (send-message! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (set-typing! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (send-reaction! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (send-file! [_ request]
    (unavailable :matrix_not_configured "Matrix client is not configured." {:request request}))
  (download-media! [_ request]
    (unavailable :media_download_unavailable "Matrix media download is not implemented yet." {:request request}))
  (transcribe-media! [_ request]
    (unavailable :transcription_unavailable "Broker-side transcription is not available." {:request request}))
  (verification-start! [_ request]
    (unavailable :verification_unavailable "Matrix verification is not implemented yet." {:request request}))
  (verification-confirm! [_ verification-id]
    (unavailable :verification_unavailable "Matrix verification is not implemented yet." {:verification-id verification-id}))
  (verification-cancel! [_ verification-id]
    (unavailable :verification_unavailable "Matrix verification is not implemented yet." {:verification-id verification-id}))
  (verification-status [_]
    {:verifications []}))

(defn disabled-gateway
  ([] (disabled-gateway "missing Matrix credentials"))
  ([reason] (->DisabledGateway reason)))

(defn configured?
  [config]
  (let [matrix (:matrix config)]
    (and (not (str/blank? (str (:homeserver-url matrix))))
         (not (str/blank? (str (:user-id matrix))))
         (or (not (str/blank? (str (:password matrix))))
             (not (str/blank? (str (:access-token matrix))))))))

(defn trixnity-gateway
  "Load the Trixnity adapter lazily.

  This keeps unit tests and disabled broker startup independent of native and
  Kotlin runtime details. Live Matrix testing can surface adapter/JDK issues at
  the edge when credentials are present."
  ([config paths]
   (trixnity-gateway config paths nil))
  ([config paths event-sink]
   (let [factory (requiring-resolve 'pi-matrix-relay.broker.matrix.trixnity/gateway)]
     (factory config paths event-sink))))

(defn gateway
  ([config paths]
   (gateway config paths nil))
  ([config paths event-sink]
   (if (configured? config)
     (trixnity-gateway config paths event-sink)
     (disabled-gateway))))
