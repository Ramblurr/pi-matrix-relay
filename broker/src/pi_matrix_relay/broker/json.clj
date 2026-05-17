(ns pi-matrix-relay.broker.json
  (:require [charred.api :as json])
  (:import [java.io InputStream InputStreamReader PushbackReader]))

(def json-content-type "application/json; charset=utf-8")

(defn read-json
  "Read a JSON string/stream into Clojure data with keyword keys.

  Empty or nil bodies return nil. This function is intentionally small so Ring
  body parsing stays at the HTTP edge."
  [body]
  (cond
    (nil? body) nil
    (string? body) (when-not (empty? body)
                     (json/read-json body :key-fn keyword))
    (instance? InputStream body) (with-open [reader (PushbackReader. (InputStreamReader. ^InputStream body "UTF-8"))]
                                   (json/read-json reader :key-fn keyword))
    :else (json/read-json body :key-fn keyword)))

(defn write-json
  [data]
  (json/write-json-str data))

(defn ok
  ([] (ok {}))
  ([data]
   {:ok true :data data}))

(defn error
  ([code message]
   (error code message {}))
  ([code message details]
   {:ok false
    :error {:code (name code)
            :message message
            :details (or details {})}}))

(defn json-response
  ([envelope]
   (json-response 200 envelope))
  ([status envelope]
   {:status status
    :headers {"Content-Type" json-content-type}
    :body (write-json envelope)}))

(defn ok-response
  ([] (ok-response {}))
  ([data] (json-response (ok data))))

(defn error-response
  ([status code message]
   (error-response status code message {}))
  ([status code message details]
   (json-response status (error code message details))))
