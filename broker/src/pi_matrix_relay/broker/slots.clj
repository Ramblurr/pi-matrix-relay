(ns pi-matrix-relay.broker.slots
  (:require [clojure.string :as str]))

(def alphabet (mapv str (map char (range (int \A) (inc (int \Z))))))

(defn slot-label
  "Converts a zero-based index to Excel-style slot labels: A..Z, AA..AZ, BA..."
  [idx]
  (when (neg? idx)
    (throw (ex-info "Slot index must be non-negative." {:idx idx})))
  (loop [n idx
         chars ()]
    (let [r (mod n 26)
          q (quot n 26)
          chars (conj chars (alphabet r))]
      (if (zero? q)
        (str/join chars)
        (recur (dec q) chars)))))

(defn first-free-slot
  [leases]
  (->> (range)
       (map slot-label)
       (drop-while #(contains? leases %))
       first))

(defn active-lease?
  [lease]
  (contains? #{:leased :suspect} (:state lease)))

(defn lease-sort-key
  [lease]
  [(:project-id lease) (:slot lease)])
