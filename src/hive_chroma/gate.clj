(ns hive-chroma.gate
  "Concurrency gates for ChromaDB reads/writes and the shared embed port.

   ChromaDB uses SQLite internally: read (4 permits) and write (1 permit)
   derefs retain their hive-weave gates. Embeddings use the ONE process-wide
   provider gate from embeddings.shared-gate, never a Chroma-only pool.
   Do not nest with-embedding-gate around a decorated provider call."
  (:require [hive-weave.gate :as g]
            [hive-chroma.host :as host]
            [malli.core :as m]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; =============================================================================
;; Gate Instances
;; =============================================================================

(defonce read-gate
  (g/gate {:permits 4 :timeout-ms 15000 :name "chroma-read"}))

(defonce write-gate
  (g/gate {:permits 1 :timeout-ms 30000 :name "chroma-write"}))

;; =============================================================================
;; Convenience API — drop-in replacements for bare @ derefs
;; =============================================================================

(def Pending
  "A deref-able vendor result (promise, future, deferred). Generates a
   realized promise."
  [:fn {:gen/schema :any :gen/fmap #(doto (promise) (deliver %))}
   #(instance? clojure.lang.IBlockingDeref %)])

(def GateStats
  "Diagnostic snapshot of the read, write and embed gates."
  [:map
   [:read :map]
   [:write :map]
   [:embed [:map
            [:name :string]
            [:permits :int]
            [:available :int]
            [:queue-length [:int {:min 0}]]]]])

(defn deref-read
  "Deref a Chroma read promise with concurrency gate + timeout.
   Replaces: @(chroma/query ...) or @(chroma/get ...)"
  ([promise]
   (g/deref-gate read-gate promise))
  ([promise timeout-ms]
   (g/deref-gate read-gate promise timeout-ms)))

(m/=> deref-read [:function
                  [:=> [:cat Pending] :any]
                  [:=> [:cat Pending [:int {:min 0}]] :any]])

(defn deref-write
  "Deref a Chroma write promise with exclusive gate + timeout.
   Replaces: @(chroma/add ...) or @(chroma/delete ...)"
  ([promise]
   (g/deref-gate write-gate promise))
  ([promise timeout-ms]
   (g/deref-gate write-gate promise timeout-ms)))

(m/=> deref-write [:function
                   [:=> [:cat Pending] :any]
                   [:=> [:cat Pending [:int {:min 0}]] :any]])

(defmacro with-embedding-gate
  "Legacy macro for callers outside the EmbeddingProvider port. Never wrap a
   decorated provider call with it: admission there already owns the permit.
   Admits through the host's ONE process-wide embedding gate; with no host,
   the body runs ungated (there is no provider to protect)."
  [& body]
  `(host/with-permit (host/process-gate) (host/lane) (fn [] ~@body)))

;; =============================================================================
;; Diagnostics
;; =============================================================================

(defn gate-stats
  "Current state of Chroma read/write and the one shared embedding provider
   gate. With no host the embed gate reports one permit, all available."
  []
  (let [embed   (host/process-gate)
        permits (or (:permits embed) 1)
        active  (if embed @(:active embed) 0)
        waiting (if embed @(:waiting embed) {})]
    {:read (g/gate-stats read-gate)
     :write (g/gate-stats write-gate)
     :embed {:name "embedding-provider"
             :permits permits
             :available (- permits active)
             :queue-length (reduce + 0 (vals waiting))}}))

(m/=> gate-stats [:=> [:cat] GateStats])
