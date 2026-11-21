(ns hive-chroma.maintenance
  "Cleanup, repair, and expiration management for Chroma entries."
  (:require [hive-chroma.client :as chroma]
            [hive-chroma.connection :as conn]
            [hive-chroma.crud :as crud]
            [hive-chroma.host :as emb]
            [hive-chroma.gate :as gate]
            [hive-chroma.helpers :as h]
            [hive-spi.log.ports :as log]
            [malli.core :as m]
            [hive-dsl.result :as r]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:private duration-days
  "Lifetime in days of each expiring canonical duration."
  {"ephemeral" 1 "short" 7 "medium" 30 "long" 90})

(def RawEntry
  "A Chroma row as `chroma/get` returns it."
  [:map [:id :any] [:metadata {:optional true} [:maybe :map]]])

(def Repair
  "The metadata updates that repair one entry."
  [:map {:closed true}
   [:duration {:optional true} :string]
   [:expires {:optional true} :string]])

(defn repair-updates
  "The metadata updates ENTRY needs, as of NOW (a ZonedDateTime), or nil when
   it needs none. A legacy duration is renamed to its canonical spelling; an
   expiring duration with no :expires gets one. Pure."
  [entry now]
  (let [raw-dur   (get-in entry [:metadata :duration])
        canonical (h/normalize-duration raw-dur)
        days      (get duration-days canonical)
        renamed?  (not= raw-dur canonical)
        expire?   (and days (or renamed? (empty? (str (get-in entry [:metadata :expires])))))
        updates   (cond-> {}
                    renamed? (assoc :duration canonical)
                    expire?  (assoc :expires (str (.plusDays now days))))]
    (not-empty updates)))

(defn- rewrite-metadata!
  "Boundary: upsert row ID of COLL with UPDATES merged into its metadata,
   keeping its vector and document. Does nothing when the row is gone."
  [coll id updates]
  (when-let [existing (first (gate/deref-read
                              (chroma/get coll :ids [id]
                                          :include #{:documents :metadatas :embeddings})))]
    (gate/deref-write
      (chroma/add coll [{:id        id
                         :embedding (:embedding existing)
                         :document  (:document existing)
                         :metadata  (merge (:metadata existing) updates)}]
                  :upsert? true))
    (log/debug "Repaired entry" id ":" updates)))

(defn- repair-entry!
  "Apply UPDATES to entry ID as a Result; a failure is logged and reported,
   never thrown, so one bad entry does not stop the pass."
  [coll id updates]
  (let [result (r/try-effect* :repair/failed (rewrite-metadata! coll id updates))]
    (when (r/err? result)
      (log/warn "Failed to repair entry" id ":" (:message result)))
    result))

(defn- repair-missing-expires!
  "Repair entries with missing duration or expires timestamps. Collect the
   entries needing repair, then rewrite each. Returns how many needed it."
  [all-entries]
  (let [now     (java.time.ZonedDateTime/now)
        repairs (keep (fn [e] (some->> (repair-updates e now) (vector (:id e)))) all-entries)]
    (when (seq repairs)
      (let [coll (conn/get-or-create-collection)]
        (doseq [[id updates] repairs]
          (repair-entry! coll id updates))))
    (count repairs)))

(def ^:private ZonedDateTime
  [:fn {:gen/elements [(java.time.ZonedDateTime/parse "2026-01-01T00:00:00Z")]}
   #(instance? java.time.ZonedDateTime %)])

(m/=> repair-updates [:=> [:cat RawEntry ZonedDateTime] [:maybe Repair]])
(m/=> rewrite-metadata! [:=> [:cat chroma/Coll :any Repair] :any])
(m/=> repair-entry! [:=> [:cat chroma/Coll :any Repair] [:map [:ok {:optional true} :any] [:error {:optional true} :keyword]]])
(m/=> repair-missing-expires! [:=> [:cat [:sequential RawEntry]] [:int {:min 0}]])

(def CleanupResult
  "What a cleanup pass did: expired ids deleted, entries repaired."
  [:map
   [:count [:int {:min 0}]]
   [:deleted-ids [:vector :string]]
   [:repaired [:int {:min 0}]]])

(defn cleanup-expired!
  "Delete expired entries and repair entries with missing expires."
  []
  (emb/require-embedding!)
  (let [coll (conn/get-or-create-collection)
        all-entries (gate/deref-read (chroma/get coll :include #{:metadatas} :limit 10000))
        repaired (try (repair-missing-expires! all-entries)
                      (catch Exception e
                        (log/warn "Repair phase failed (non-blocking):" (.getMessage e))
                        0))
        entries-to-check (if (pos? repaired)
                           (gate/deref-read (chroma/get coll :include #{:metadatas} :limit 10000))
                           all-entries)
        expired-ids (->> entries-to-check
                         (filter #(h/expired? (:metadata %)))
                         (map :id)
                         vec)]
    (when (seq expired-ids)
      (gate/deref-write (chroma/delete coll :ids expired-ids))
      (log/info "Cleaned up" (count expired-ids) "expired entries"))
    (when (pos? repaired)
      (log/info "Repaired" repaired "entries with missing/invalid duration or expires"))
    {:count (count expired-ids)
     :deleted-ids expired-ids
     :repaired repaired}))

(m/=> cleanup-expired! [:=> [:cat] CleanupResult])

(defn entries-expiring-soon
  "Get entries expiring within the given number of days."
  [days & {:keys [project-id]}]
  (emb/require-embedding!)
  (let [now (java.time.ZonedDateTime/now)
        threshold (.plusDays now days)
        entries (crud/query-entries :project-id project-id :limit 10000 :include-expired? false)]
    (->> entries
         (filter (fn [entry]
                   (when-let [exp-time (h/parse-zoned-datetime (:expires entry))]
                     (h/time-between? exp-time now threshold))))
         (sort-by :expires)
         vec)))

(m/=> entries-expiring-soon [:=> [:cat number? [:alt [:* [:cat :keyword :any]] [:cat :map]]]
                             [:vector crud/Entry]])
