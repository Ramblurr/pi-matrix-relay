(ns pi-matrix-relay.broker.response)

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

(defn edn-response
  ([envelope]
   (edn-response 200 envelope))
  ([status envelope]
   {:status status
    :body envelope}))

(defn ok-response
  ([] (ok-response {}))
  ([data] (edn-response (ok data))))

(defn error-response
  ([status code message]
   (error-response status code message {}))
  ([status code message details]
   (edn-response status (error code message details))))
