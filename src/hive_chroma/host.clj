(ns hive-chroma.host
  "Every HOST service the Chroma backend consumes, in one place.

   hive-chroma is an addon: its host (hive-mcp) is provided at runtime and is
   never a compile-time dependency of a namespace under src/. Each host fn is
   soft-resolved through its var at call time (hive-addon.host), so every
   namespace here loads with no host on the classpath, and each declares the
   degraded value its call site expects:

     - a lookup (provider, route, space, config value) answers nil,
     - a predicate answers false,
     - a pure shaping helper falls back to the identity of its input,
     - an embed call or the embedding guard THROWS, because a write or a
       search that silently skipped its vector would store or answer garbage.

   `hive-addon.host/unresolvable` over this namespace must be empty whenever
   the host is on the classpath: test/hive_chroma/host_seam_test.clj checks
   it, so a host rename fails the suite instead of degrading forever."
  (:require [hive-addon.host :as host]
            [malli.core :as m]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defn- host-absent!
  "Throw the one host-absent failure an embed path may raise."
  [sym]
  (throw (ex-info (str "hive-chroma: host service unavailable: " sym)
                  {:error :hive-chroma/host-absent :host/sym (str sym)})))

;; =============================================================================
;; Active embedding provider (hive-mcp.embeddings.active)
;; =============================================================================

(host/defsoft require-embedding!
  'hive-mcp.embeddings.active/require-embedding!
  :absent (fn [] (throw (ex-info "Embedding provider not configured (host absent)"
                                 {:type :no-embedding-provider}))))

(host/defsoft embedding-configured?
  'hive-mcp.embeddings.active/embedding-configured? :absent (constantly false))

(host/defsoft get-embedding-provider
  'hive-mcp.embeddings.active/get-embedding-provider :absent (constantly nil))

(host/defsoft set-embedding-provider!
  'hive-mcp.embeddings.active/set-embedding-provider! :absent (constantly nil))

(host/defsoft reset-embedding-provider!
  'hive-mcp.embeddings.active/reset-embedding-provider! :absent (constantly nil))

(host/defsoft embedding-dimension
  'hive-mcp.embeddings.active/embedding-dimension
  :absent (fn [_] (host-absent! 'hive-mcp.embeddings.active/embedding-dimension)))

(host/defsoft embed-text
  'hive-mcp.embeddings.active/embed-text
  :absent (fn [_ _] (host-absent! 'hive-mcp.embeddings.active/embed-text)))

(host/defsoft embed-batch
  'hive-mcp.embeddings.active/embed-batch
  :absent (fn [_ _] (host-absent! 'hive-mcp.embeddings.active/embed-batch)))

(host/defsoft get-provider-for
  'hive-mcp.embeddings.active/get-provider-for :absent (constantly nil))

(host/defsoft embed-text-for
  'hive-mcp.embeddings.active/embed-text-for
  :absent (fn [_ _] (host-absent! 'hive-mcp.embeddings.active/embed-text-for)))

(host/defsoft embed-batch-for
  'hive-mcp.embeddings.active/embed-batch-for
  :absent (fn [_ _] (host-absent! 'hive-mcp.embeddings.active/embed-batch-for)))

(host/defsoft get-dimension-for
  'hive-mcp.embeddings.active/get-dimension-for :absent (constantly nil))

;; =============================================================================
;; Per-type routing (hive-mcp.embeddings.service)
;; =============================================================================

(host/defsoft no-embed-type?
  'hive-mcp.embeddings.service/no-embed-type? :absent (constantly false))

(host/defsoft resolve-provider-for-type
  'hive-mcp.embeddings.service/resolve-provider-for-type :absent (constantly nil))

(host/defsoft resolve-provider-for-type+size
  'hive-mcp.embeddings.service/resolve-provider-for-type+size :absent (constantly nil))

(host/defsoft validate-content-size!
  'hive-mcp.embeddings.service/validate-content-size! :absent (constantly nil))

(host/defsoft space-for-resolved
  'hive-mcp.embeddings.service/space-for-resolved :absent (constantly nil))

(host/defsoft default-space
  'hive-mcp.embeddings.service/default-space :absent (constantly nil))

(host/defsoft sibling-failover
  'hive-mcp.embeddings.service/sibling-failover :absent (constantly nil))

(host/defsoft type->collection-names
  'hive-mcp.embeddings.service/type->collection-names :absent (constantly nil))

(host/defsoft all-collection-names
  'hive-mcp.embeddings.service/all-collection-names :absent (constantly []))

;; =============================================================================
;; Embedding spaces (hive-mcp.embeddings.space)
;; =============================================================================

(host/defsoft space->metadata
  'hive-mcp.embeddings.space/space->metadata :absent (constantly nil))

(host/defsoft admit
  'hive-mcp.embeddings.space/admit
  :absent (fn [_coll-name _space _metadata] {:verdict :unchecked :space-id nil}))

(host/defsoft audit
  'hive-mcp.embeddings.space/audit
  :absent (fn [coll-name & _] {:collection coll-name :verdict :unchecked
                               :space-id nil :remedy nil}))

;; Schema stand-ins: the host owns the EmbeddingSpace and AuditRow schemas;
;; here a space is any map (or nil when the provider cannot name one), and an
;; audit row is any map.
(def EmbeddingSpace [:maybe :map])
(def AuditRow :map)

;; =============================================================================
;; Query shaping, config, federated ingest search
;; =============================================================================

(host/defsoft shape-query
  'hive-mcp.embeddings.role/shape-query :absent (fn [_template text] text))

(host/defsoft get-in-config
  'hive-mcp.config.core/get-in-config :absent (constantly nil))

(host/defsoft resolve-ingest-search
  'hive-mcp.memory.ingest-search/resolve-ingest-search :absent (constantly nil))

(host/defsoft normalize-ingest-results
  'hive-mcp.memory.ingest-search/normalize-ingest-results :absent (constantly []))

(host/defsoft merge-and-rerank
  'hive-mcp.memory.ingest-search/merge-and-rerank
  :absent (fn [default-results _ingest limit]
            (->> default-results
                 (sort-by #(or (:distance %) 999.0))
                 (take limit)
                 vec)))

;; =============================================================================
;; The one process-wide embedding gate (hive-mcp.embeddings.shared-gate)
;; =============================================================================

(host/defsoft with-permit
  'hive-mcp.embeddings.shared-gate/with-permit :absent (fn [_gate _lane f] (f)))

(defn process-gate
  "The host's process-wide embedding gate, or nil when the host is absent."
  []
  (some-> (host/resolve-var 'hive-mcp.embeddings.shared-gate/process-gate) deref))

(defn lane
  "The caller's current embedding lane, or nil when the host is absent."
  []
  (some-> (host/resolve-var 'hive-mcp.embeddings.shared-gate/*lane*) deref))

;; =============================================================================
;; The host's named-collection port (hive-mcp.protocols.vector)
;; =============================================================================

(defn vector-port
  "The host's IVectorCollectionStore protocol, else the published
   hive-spi.vector.ports one, else nil. Resolved at factory time: the port is
   a runtime presence, not a compile dependency."
  []
  (some-> (or (host/resolve-var 'hive-mcp.protocols.vector/IVectorCollectionStore)
              (host/resolve-var 'hive-spi.vector.ports/IVectorCollectionStore))
          deref))

(defn install-vector-store!
  "Install STORE as the host's vector-collection store when the host's port
   is present and no store is installed yet. Answers :installed, :kept (one
   was already installed; an addon never overrides another backend) or
   :host-absent."
  [store]
  (let [store-set? (host/resolve-var 'hive-mcp.protocols.vector/store-set?)
        set-store! (host/resolve-var 'hive-mcp.protocols.vector/set-store!)]
    (cond
      (not (and store-set? set-store!)) :host-absent
      (@store-set?)                     :kept
      :else                             (do (@set-store! store) :installed))))

(m/=> process-gate [:=> [:cat] [:maybe :map]])
(m/=> lane [:=> [:cat] :any])
(m/=> vector-port [:=> [:cat] [:maybe :map]])
(m/=> install-vector-store! [:=> [:cat :any] [:enum :installed :kept :host-absent]])
