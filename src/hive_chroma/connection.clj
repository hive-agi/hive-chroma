(ns hive-chroma.connection
  "Chroma connection configuration, collection management, and health status."
  (:require [hive-chroma.client :as chroma]
            [hive-chroma.host :as emb]
            [hive-chroma.host :as embedding-service]
            [hive-chroma.host :as space]
            [hive-spi.log.ports :as log] [hive-dsl.result :refer [rescue]]
            [malli.core :as m]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:private default-config
  {:host "localhost"
   :port 8000
   :collection-name "hive-mcp-memory"})

(defonce ^:private config (atom default-config))

(def Config
  "Chroma connection settings; callers may merge in vendor options."
  [:map
   [:host :string]
   [:port :int]
   [:collection-name :string]])

(def Status
  "Chroma integration status."
  [:map
   [:configured? :boolean]
   [:provider [:maybe :string]]
   [:collection [:maybe :string]]
   [:host [:maybe :string]]
   [:port [:maybe :int]]])

(defn get-config
  "Get current Chroma configuration map."
  []
  @config)

(m/=> get-config [:=> [:cat] Config])

(defn configure!
  "Configure Chroma connection settings."
  [opts]
  (swap! config merge opts)
  (chroma/configure (select-keys opts [:host :port :api-version :tenant :database]))
  (log/info "Chroma configured:" (select-keys @config [:host :port :collection-name])))

(m/=> configure! [:=> [:cat [:map
                             [:host {:optional true} :string]
                             [:port {:optional true} :int]
                             [:collection-name {:optional true} :string]]]
                  :nil])

(defonce ^:private collection-cache (atom nil))

(defn- try-get-collection
  "Try to get existing collection, returns nil on failure."
  [coll-name]
  (rescue nil (deref (chroma/get-collection coll-name) 10000 nil)))

(defn- create-new-collection
  "Create a new Chroma collection with dimension metadata, plus the embedding
   space `stamp` (space/space->metadata) when the caller can name one."
  ([coll-name dim] (create-new-collection coll-name dim nil))
  ([coll-name dim stamp]
   (let [result (deref (chroma/create-collection
                        coll-name {:metadata (merge {:dimension dim :created-by "hive-mcp"} stamp)})
                       15000 ::timeout)]
     (when (= result ::timeout)
       (throw (ex-info "Chroma create-collection timed out" {:collection coll-name})))
     result)))

(defn- cache-collection!
  "Cache and return collection, logging action."
  [coll log-msg]
  (reset! collection-cache coll)
  (log/info log-msg)
  coll)

(defonce ^:private named-collection-cache (atom {}))

(def metric
  "Distance Chroma scores a collection with when no hnsw:space is given."
  :l2)

(defonce ^:private legacy-warned (atom #{}))

(declare admit-space!)

(defn get-or-create-collection
  "Get the default (unnamed) collection or create it. A new one is stamped
   with the global provider's space when that space can be named.

   With `space` (an EmbeddingSpace) the collection must hold it, as in
   `get-or-create-named-collection`: a stamped collection of another space
   throws ex-info :embedding-space-mismatch, an unstamped (legacy) one is
   admitted with a warning. No argument, or a nil `space`, skips the guard."
  ([] (get-or-create-collection nil))
  ([space]
   (let [coll-name (:collection-name @config)
         coll (or @collection-cache
                  (let [_ (emb/require-embedding!)
                        dim (emb/embedding-dimension (emb/get-embedding-provider))]
                    (if-let [existing (try-get-collection coll-name)]
                      (cache-collection! existing (str "Using existing Chroma collection: " coll-name))
                      (cache-collection!
                       (create-new-collection
                        coll-name dim
                        (some-> (or space (embedding-service/default-space {:metric metric}))
                                space/space->metadata))
                       (str "Created Chroma collection: " coll-name " dimension: " dim)))))]
     (if space (admit-space! coll-name coll space) coll))))

(m/=> get-or-create-collection
      [:function
       [:=> [:cat] chroma/Coll]
       [:=> [:cat [:maybe space/EmbeddingSpace]] chroma/Coll]])

(defn- admit-space!
  "Guard `coll` (named `coll-name`) for vectors of `space`. A stamped
   collection of another space throws :embedding-space-mismatch; an unstamped
   one is legacy: admitted, with one warning per collection per process."
  [coll-name coll space]
  (let [{:keys [verdict space-id]} (space/admit coll-name space (when (map? coll) (:metadata coll)))]
    (when (and (= :legacy verdict) (not (contains? @legacy-warned coll-name)))
      (swap! legacy-warned conj coll-name)
      (log/warn "Embedding space unrecorded for legacy collection" coll-name
                "- admitting vectors of space" space-id
                "unverified; isolate or re-embed to stamp it"))
    coll))

(defn get-or-create-named-collection
  "Get or create a named collection with explicit dimension.
   Caches per collection-name. Used by type-based embedder routing.

   With `space` (an EmbeddingSpace) a new collection is stamped with it, and
   an existing one must hold it: a stamped collection of another space throws
   ex-info :embedding-space-mismatch, so vectors of two spaces never share a
   collection; an unstamped (legacy) collection is admitted with a warning.
   A nil `space` (provider without a nameable space) skips the guard."
  ([coll-name dimension] (get-or-create-named-collection coll-name dimension nil))
  ([coll-name dimension space]
   (let [coll (or (get @named-collection-cache coll-name)
                  (let [coll (or (try-get-collection coll-name)
                                 (create-new-collection coll-name dimension
                                                        (some-> space space/space->metadata)))]
                    (swap! named-collection-cache assoc coll-name coll)
                    (log/info "Named collection ready:" coll-name "dimension:" dimension)
                    coll))]
     (if space (admit-space! coll-name coll space) coll))))

(m/=> get-or-create-named-collection
      [:function
       [:=> [:cat :string pos-int?] chroma/Coll]
       [:=> [:cat :string pos-int? [:maybe space/EmbeddingSpace]] chroma/Coll]])

(defn reset-collection-cache!
  "Reset the collection cache (for testing/reconnection)."
  []
  (reset! collection-cache nil)
  (reset! legacy-warned #{})
  (reset! named-collection-cache {}))

(m/=> reset-collection-cache! [:=> [:cat] [:map-of {:max 0} :any :any]])

(defn collection-names
  "Every collection this process could legitimately read: the default
   (unnamed) collection plus every collection a configured provider's dimension
   writes to. `extra` adds names the caller knows about (legacy collections a
   scan visits). Config only: it opens nothing and creates nothing."
  ([] (collection-names nil))
  ([extra]
   (vec (distinct (concat [(:collection-name @config)]
                          (rescue [] (embedding-service/all-collection-names))
                          extra)))))

(defn collection-space-audit
  "Provenance of every collection this process could read, as data: one row per
   collection that EXISTS, from `space/audit`. Never creates a collection and
   never throws on a mismatch: this is the listing an operator uses to find the
   collections that need isolating or re-embedding, which is why a divergent one
   is reported :diverged instead of refused.

   Opts:
     :names - extra collection names to include (legacy/sibling ones)
     :space - the EmbeddingSpace a caller is about to use; with it each row also
              carries :expected-id and the keys a divergent collection differs on."
  ([] (collection-space-audit nil))
  ([opts]
   (let [expected (:space opts)]
     (vec (keep (fn [n]
                  (when-let [coll (try-get-collection n)]
                    (space/audit n expected (:metadata coll))))
                (collection-names (:names opts)))))))

(m/=> collection-names [:function [:=> [:cat] [:vector :string]]
                        [:=> [:cat [:maybe [:sequential :string]]] [:vector :string]]])

(m/=> collection-space-audit
      [:function
       [:=> [:cat] [:sequential space/AuditRow]]
       [:=> [:cat [:maybe [:map
                           [:names {:optional true} [:maybe [:sequential :string]]]
                           [:space {:optional true} [:maybe [:map-of :keyword :any]]]]]]
        [:sequential space/AuditRow]]])

(defn status
  "Get Chroma integration status."
  []
  {:configured? (emb/embedding-configured?)
   :provider (when-let [p (emb/get-embedding-provider)] (str (type p)))
   :collection (:collection-name @config)
   :host (:host @config)
   :port (:port @config)})

(m/=> status [:=> [:cat] Status])

(defn chroma-available?
  "Check if Chroma is configured and reachable."
  []
  (when (emb/embedding-configured?)
    (try
      (get-or-create-collection)
      true
      (catch Exception e
        (log/debug "Chroma availability check failed:" (.getMessage e))
        false))))

(m/=> chroma-available? [:=> [:cat] [:maybe :boolean]])

(defn provider-spec
  "Build the embedding space spec for a provider, including its resolved host
   when present. Return nil when the provider has no configured model."
  [provider-type model provider]
  (when model
    (cond-> {:impl provider-type :model (str model)}
      (:host provider) (assoc :host (str (:host provider))))))

(m/=> provider-spec
      [:=> [:cat :keyword :any [:maybe [:map [:host {:optional true} :any]]]]
       [:maybe [:map
                [:impl :keyword]
                [:model :string]
                [:host {:optional true} :string]]]])
