(ns hive-chroma.host
  "Every HOST service the Chroma backend consumes, in one place.

   hive-chroma depends on hive-spi and never on its host by name: not at
   compile time, and not by resolving host symbols at runtime. The host
   publishes what a vector-store backend needs as capability -> fn through
   `hive-spi.vector.host-services` (hive-mcp does it in
   hive-mcp.embeddings.host-services), and each fn below calls through that
   registry at CALL time. The pure embedding-space layer needs no host at all
   and comes straight from `hive-spi.embeddings.space`.

   Every namespace here loads with no host, and each call site declares the
   degraded value it expects when the host published nothing:

     - a lookup (provider, route, space, config value) answers nil,
     - a predicate answers false,
     - a pure shaping helper falls back to the identity of its input,
     - an embed call or the embedding guard THROWS, because a write or a
       search that silently skipped its vector would store or answer garbage."
  (:require [hive-spi.embeddings.space :as spi-space]
            [hive-spi.vector.host-services :as hs]
            [hive-spi.vector.ports :as vp]
            [malli.core :as m]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defn- host-absent!
  "Throw the one host-absent failure an embed path may raise."
  [cap]
  (throw (ex-info (str "hive-chroma: host service unavailable: " cap)
                  {:error :hive-chroma/host-absent :host/capability cap})))

(defn- absent-thrower [cap] (fn [& _] (host-absent! cap)))

(defmacro ^:private defservice
  "Def NAME as the host capability CAP, called through
   `hive-spi.vector.host-services/soft` with ABSENT as its fallback. The var
   carries CAP as :host/capability, so `consumed-capabilities` can list what
   this backend asks of its host."
  [name cap absent]
  `(def ~(vary-meta name assoc :host/capability cap)
     (hs/soft ~cap ~absent)))

;; =============================================================================
;; Active embedding provider
;; =============================================================================

(defservice require-embedding! :embedding/require!
  (fn [] (throw (ex-info "Embedding provider not configured (host absent)"
                         {:type :no-embedding-provider}))))

(defservice embedding-configured? :embedding/configured? (constantly false))
(defservice get-embedding-provider :embedding/provider (constantly nil))
(defservice set-embedding-provider! :embedding/set-provider! (constantly nil))
(defservice reset-embedding-provider! :embedding/reset-provider! (constantly nil))
(defservice embedding-dimension :embedding/dimension (absent-thrower :embedding/dimension))
(defservice embed-text :embedding/embed-text (absent-thrower :embedding/embed-text))
(defservice embed-batch :embedding/embed-batch (absent-thrower :embedding/embed-batch))
(defservice get-provider-for :embedding/provider-for (constantly nil))
(defservice embed-text-for :embedding/embed-text-for (absent-thrower :embedding/embed-text-for))
(defservice embed-batch-for :embedding/embed-batch-for (absent-thrower :embedding/embed-batch-for))
(defservice get-dimension-for :embedding/dimension-for (constantly nil))

;; =============================================================================
;; Per-type routing
;; =============================================================================

(defservice no-embed-type? :routing/no-embed-type? (constantly false))
(defservice resolve-provider-for-type :routing/resolve-for-type (constantly nil))
(defservice resolve-provider-for-type+size :routing/resolve-for-type+size (constantly nil))
(defservice validate-content-size! :routing/validate-content-size! (constantly nil))
(defservice space-for-resolved :routing/space-for-resolved (constantly nil))
(defservice default-space :routing/default-space (constantly nil))
(defservice sibling-failover :routing/sibling-failover (constantly nil))
(defservice type->collection-names :routing/type->collection-names (constantly nil))
(defservice all-collection-names :routing/all-collection-names (constantly []))

;; =============================================================================
;; Embedding spaces: pure, from hive-spi
;; =============================================================================

(def space->metadata spi-space/space->metadata)
(def admit spi-space/admit)
(def audit spi-space/audit)

;; A space as this backend's contracts see it: any map, or nil when the
;; provider cannot name one. The full schema is hive-spi's.
(def EmbeddingSpace [:maybe :map])
(def AuditRow spi-space/AuditRow)

;; =============================================================================
;; Query shaping, config, federated ingest search
;; =============================================================================

(defservice shape-query :query/shape (fn [_template text] text))
(defservice get-in-config :config/get-in (constantly nil))
(defservice resolve-ingest-search :ingest/resolve-search (constantly nil))
(defservice normalize-ingest-results :ingest/normalize-results (constantly []))

(defservice merge-and-rerank :ingest/merge-and-rerank
  (fn [default-results _ingest limit]
    (->> default-results
         (sort-by #(or (:distance %) 999.0))
         (take limit)
         vec)))

;; =============================================================================
;; The one process-wide embedding gate
;; =============================================================================

(defservice with-permit :gate/with-permit (fn [_gate _lane f] (f)))
(defservice ^:private process-gate* :gate/process-gate (constantly nil))
(defservice ^:private lane* :gate/lane (constantly nil))

(defn process-gate
  "The host's process-wide embedding gate, or nil when the host published none."
  []
  (process-gate*))

(defn lane
  "The caller's current embedding lane, or nil when the host published none."
  []
  (lane*))

;; =============================================================================
;; The host's named-collection port
;; =============================================================================

(defservice ^:private vector-port* :vector/port (constantly nil))

(defn vector-port
  "The IVectorCollectionStore protocol the host consumes, else the published
   hive-spi.vector.ports one. Resolved at factory time."
  []
  (or (vector-port*) vp/IVectorCollectionStore))

(def ^:private store-capabilities
  "What `install-vector-store!` asks of the host."
  {:host/capability [:vector/store-set? :vector/set-store!]})

(defn install-vector-store!
  "Install STORE as the host's vector-collection store when the host
   publishes its store slot and no store is installed yet. Answers :installed,
   :kept (one was already installed; an addon never overrides another backend)
   or :host-absent."
  [store]
  (let [store-set? (hs/capability :vector/store-set?)
        set-store! (hs/capability :vector/set-store!)]
    (cond
      (not (and store-set? set-store!)) :host-absent
      (store-set?)                      :kept
      :else                             (do (set-store! store) :installed))))

(defn consumed-capabilities
  "Every host capability this backend asks for, as a set. Each must be a key
   of `hive-spi.vector.host-services/catalog`."
  []
  (into (set (:host/capability store-capabilities))
        (keep (comp :host/capability meta))
        (vals (ns-interns 'hive-chroma.host))))

(m/=> process-gate [:=> [:cat] [:maybe :map]])
(m/=> lane [:=> [:cat] :any])
(m/=> vector-port [:=> [:cat] [:maybe :map]])
(m/=> install-vector-store! [:=> [:cat :any] [:enum :installed :kept :host-absent]])
(m/=> consumed-capabilities [:=> [:cat] [:set :keyword]])
