(ns pi-matrix-relay.broker.json
  (:require [charred.api :as json])
  (:import [java.io InputStream InputStreamReader PushbackReader]))

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
