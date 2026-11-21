(ns hive-chroma.test.host
  "A stub HOST for the suite, published through hive-spi exactly as a real host
   publishes itself (`hive-spi.vector.host-services`). No hive-mcp anywhere.

   It models the slice of host behaviour the backend's contracts depend on:

     - one global embedding provider, optionally set with a spec naming its
       model (`set-embedding-provider!`),
     - per-type routing over an EDN `:embedder` config ({:default k :routes
       {type k} :providers {k spec}}), provider instances built by
       `*provider-factory*`,
     - the embedding space of a route or of the global provider, with the
       model digest answered by a swappable resolver,
     - config lookup over `*config*`,
     - the host's named-collection port (hive-spi.vector.ports).

   Load this namespace and the host is published; `install!` re-publishes it
   after a test cleared the registry."
  (:require [hive-spi.embeddings.ports :as emb]
            [hive-spi.embeddings.space :as space]
            [hive-spi.vector.host-services :as hs]
            [hive-spi.vector.ports :as vp]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:dynamic *config*
  "The effective host config (see hive-chroma.test.fixtures/with-config)."
  {})

(def ^:dynamic *provider-factory*
  "(fn [provider-spec]) -> EmbeddingProvider for a routed provider."
  (fn [_spec] nil))

(def doc-serializer-rev
  "Revision of the embedded document text."
  "memory-to-document/1")

(def base-collection-name "hive-mcp-memory")

(defonce ^:private state (atom {:provider nil :spec nil}))
(defonce ^:private digest-resolver (atom (fn [_spec] nil)))

;; =============================================================================
;; Global provider
;; =============================================================================

(defn set-embedding-provider!
  ([provider] (set-embedding-provider! provider nil))
  ([provider spec]
   (reset! state {:provider provider :spec (when provider spec)})
   nil))

(defn reset-embedding-provider! [] (reset! state {:provider nil :spec nil}) nil)
(defn get-embedding-provider [] (:provider @state))
(defn get-embedding-provider-spec [] (:spec @state))
(defn embedding-configured? [] (some? (:provider @state)))

(defn require-embedding! []
  (when-not (:provider @state)
    (throw (ex-info "Embedding provider not configured."
                    {:type :no-embedding-provider}))))

(defn set-digest-resolver!
  "Install the digest port: (fn [provider-spec]) -> digest or nil."
  [f]
  (reset! digest-resolver f)
  f)

;; =============================================================================
;; Routing and spaces
;; =============================================================================

(defn- embedder-config [] (:embedder *config*))

(defn- collection-name-for-dimension [dimension]
  (if (= dimension 768)
    base-collection-name
    (str base-collection-name "-" dimension "d")))

(defn- build-resolved [provider-key]
  (if-let [spec (get-in (embedder-config) [:providers provider-key])]
    {:provider        (*provider-factory* spec)
     :dimension       (:dimension spec)
     :max-tokens      (:max-tokens spec 8192)
     :collection-name (collection-name-for-dimension (:dimension spec))
     :provider-key    provider-key}
    (let [global (get-embedding-provider)]
      (when-not global
        (throw (ex-info "No embedding provider available" {:provider-key provider-key})))
      {:provider        global
       :dimension       (emb/embedding-dimension global)
       :max-tokens      2048
       :collection-name base-collection-name
       :provider-key    :fallback})))

(defn resolve-provider-for-type [memory-type]
  (let [cfg (embedder-config)]
    (build-resolved (or (get (:routes cfg) (some-> memory-type name)) (:default cfg)))))

(defn resolve-provider-for-type+size [memory-type _content]
  (resolve-provider-for-type memory-type))

(defn no-embed-type? [memory-type]
  (boolean (when memory-type
             (contains? (into #{} (map name) (:no-embed-types (embedder-config)))
                        (name memory-type)))))

(defn all-collection-names []
  (->> (:providers (embedder-config)) vals (keep :dimension)
       (map collection-name-for-dimension) distinct vec))

(defn type->collection-names [memory-type]
  (if memory-type
    [(:collection-name (resolve-provider-for-type memory-type))]
    (all-collection-names)))

(defn- query-template-rev []
  (let [t (get-in *config* [:embeddings :query-template])]
    (cond (nil? t) "raw" (keyword? t) (name t) (string? t) t :else (pr-str t))))

(defn- spec->space [spec dimension extra]
  (when (and (:impl spec) (:model spec) dimension)
    (space/->space (merge {:provider           (:impl spec)
                           :model              (str (:model spec))
                           :digest             (or (:digest spec) (@digest-resolver spec))
                           :dimension          dimension
                           :query-template-rev (query-template-rev)
                           :doc-serializer-rev doc-serializer-rev}
                          (select-keys spec [:metric :normalized?])
                          extra))))

(defn default-space
  ([] (default-space nil))
  ([extra]
   (when-let [provider (get-embedding-provider)]
     (let [spec (get-embedding-provider-spec)
           s    (spec->space spec (emb/embedding-dimension provider) extra)]
       (when (or (not= :ollama (:impl spec)) (:digest s))
         s)))))

(defn space-for-resolved
  ([resolved] (space-for-resolved resolved nil))
  ([{:keys [provider-key dimension]} extra]
   (if (= :fallback provider-key)
     (default-space extra)
     (when-let [spec (get-in (embedder-config) [:providers provider-key])]
       (spec->space spec (or dimension (:dimension spec)) extra)))))

(defn get-in-config
  ([path] (get-in *config* path))
  ([path default] (get-in *config* path default)))

;; =============================================================================
;; The one process-wide embedding gate: one permit, shared by every caller
;; =============================================================================

(defonce process-gate
  {:permits 1
   :active  (atom 0)
   :waiting (atom {})
   :sem     (java.util.concurrent.Semaphore. 1 true)})

(defn with-permit
  "Call F holding a permit of GATE."
  [gate _lane f]
  (let [^java.util.concurrent.Semaphore sem (:sem gate)]
    (.acquire sem)
    (swap! (:active gate) inc)
    (try (f)
         (finally
           (swap! (:active gate) dec)
           (.release sem)))))

;; =============================================================================
;; Publication
;; =============================================================================

(defn capabilities []
  {:embedding/require!             require-embedding!
   :embedding/configured?          embedding-configured?
   :embedding/provider             get-embedding-provider
   :embedding/set-provider!        set-embedding-provider!
   :embedding/reset-provider!      reset-embedding-provider!
   :embedding/dimension            emb/embedding-dimension
   :embedding/embed-text           emb/embed-text
   :embedding/embed-batch          emb/embed-batch
   :routing/no-embed-type?         no-embed-type?
   :routing/resolve-for-type       resolve-provider-for-type
   :routing/resolve-for-type+size  resolve-provider-for-type+size
   :routing/validate-content-size! (fn [_text _resolved] nil)
   :routing/space-for-resolved     space-for-resolved
   :routing/default-space          default-space
   :routing/sibling-failover       (fn [_resolved] nil)
   :routing/type->collection-names type->collection-names
   :routing/all-collection-names   all-collection-names
   :embedding/provider-for         (fn [_coll] (get-embedding-provider))
   :embedding/embed-text-for       (fn [_coll text] (emb/embed-text (get-embedding-provider) text))
   :embedding/embed-batch-for      (fn [_coll texts] (emb/embed-batch (get-embedding-provider) texts))
   :embedding/dimension-for        (fn [_coll] (some-> (get-embedding-provider) emb/embedding-dimension))
   :query/shape                    (fn [_template text] text)
   :config/get-in                  get-in-config
   :ingest/resolve-search          (fn [] nil)
   :ingest/normalize-results       (fn [_results] [])
   :ingest/merge-and-rerank        (fn [default-results _ingest limit]
                                     (->> default-results
                                          (sort-by #(or (:distance %) 999.0))
                                          (take limit)
                                          vec))
   :gate/with-permit               with-permit
   :gate/process-gate              (fn [] process-gate)
   :gate/lane                      (fn [] nil)
   :vector/port                    (fn [] vp/IVectorCollectionStore)
   :vector/store-set?              vp/store-set?
   :vector/set-store!              vp/set-store!})

(defn install!
  "Publish the stub host. Returns the capability keys."
  []
  (set (keys (hs/register-services! (capabilities)))))

(install!)
