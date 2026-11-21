(ns hive-chroma.search
  "Semantic search operations for Chroma memory entries."
  (:require [hive-chroma.client :as chroma]
            [hive-chroma.connection :as conn]
            [hive-chroma.gate :as gate]
            [hive-chroma.host :as host]
            [hive-dsl.result :as r]
            [hive-spi.log.ports :as log]
            [malli.core :as m]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def TagSet
  "Tags or project ids as callers hold them: any sequential or a set."
  [:maybe [:or [:sequential :any] [:set :any]]])

(def KwOpts
  "Trailing keyword options (`& {:keys ...}`): key/value pairs, or one map."
  [:alt [:* [:cat :keyword :any]] [:cat :map]])

(def WhereFilters
  "The filters build-where-clause reads."
  [:map
   [:type {:optional true} :any]
   [:project-ids {:optional true} TagSet]])

(def WhereClause
  "A Chroma metadata where clause, nil when no filter applies."
  [:maybe [:map
           [:type {:optional true} some?]
           [:project-id {:optional true} [:map [:$in [:vector :any]]]]]])

(def NotContains
  "One document-level tag exclusion."
  [:map {:closed true} [:$not_contains :any]])

(def WhereDocumentClause
  "Document-level exclusion: one $not_contains, or an $and of several."
  [:maybe [:or
           NotContains
           [:map {:closed true} [:$and [:vector {:min 2} NotContains]]]]])

(def Hit
  "A search result record as the vendor returns it."
  [:map [:id :any]])

(defn build-where-clause
  "Build Chroma where clause from type and project-id filters.
   Pure function — no IO."
  [{:keys [type project-ids]}]
  (let [base (cond-> {}
               type        (assoc :type type)
               project-ids (assoc :project-id {:$in (vec project-ids)}))]
    (when (seq base) base)))

(m/=> build-where-clause [:=> [:cat WhereFilters] WhereClause])

(defn build-where-document-clause
  "Build Chroma where-document clause for tag exclusion.
   Uses document-level $not_contains since tags are embedded in document text.
   Pure function — no IO."
  [exclude-tags]
  (when (seq exclude-tags)
    (if (= 1 (count exclude-tags))
      {:$not_contains (first exclude-tags)}
      {:$and (mapv (fn [t] {:$not_contains t}) exclude-tags)})))

(m/=> build-where-document-clause [:=> [:cat TagSet] WhereDocumentClause])

(def Query
  "One collection query: result limit and the metadata and document filters.
   The query vector is added per route, since each route embeds with its own
   provider."
  [:map
   [:limit pos-int?]
   [:where WhereClause]
   [:where-document WhereDocumentClause]
   [:embedding {:optional true} [:sequential number?]]])

(def SearchResult
  "A search attempt: {:ok hits} or a failure carrying its message and ex-data."
  [:or
   [:map [:ok [:sequential Hit]]]
   [:map [:error :keyword] [:message {:optional true} [:maybe :string]] [:data {:optional true} :any]]])

(defn- failure
  "The Result of a search step that threw E."
  [category e]
  (r/err category {:message (ex-message e) :data (ex-data e)}))

(defn- search-collection
  "Boundary: run QUERY (with its :embedding) against COLL, as a Result."
  [coll {:keys [embedding limit where where-document]}]
  (try
    (r/ok (gate/deref-read (chroma/query coll embedding
                                         :num-results limit
                                         :where where
                                         :where-document where-document
                                         :include #{:documents :metadatas :distances})))
    (catch Exception e
      (failure :search/collection-failed e))))

(defn- hits-or-empty
  "The hits of RESULT, else [] after logging why collection CN was left out.
   A space mismatch is a refusal and logs at :warn; any other failure is
   transient and logs at :debug."
  [cn result]
  (if (r/ok? result)
    (:ok result)
    (do (if (= :embedding-space-mismatch (get-in result [:data :error]))
          (log/warn "Search skipped" cn ":" (:message result))
          (log/debug "Search failed for" cn ":" (:message result)))
        [])))

(defn- shape-query
  "QUERY-TEXT shaped by TEMPLATE, else the configured
   embeddings.query-template, else :raw."
  [query-text template]
  (host/shape-query (or template (host/get-in-config [:embeddings :query-template]) :raw)
                    query-text))

(defn- query-route
  "Embed SHAPED with ROUTE's provider (the global one with no route) and run
   QUERY against ROUTE's space-guarded collection. A failing collection or
   embed throws; a failing query is a Result."
  [route shaped query]
  (let [provider (if route (:provider route) (host/get-embedding-provider))
        coll     (conn/collection-for-route route)]
    (search-collection coll (assoc query :embedding (host/embed-text provider shaped)))))

(defn- fan-out-route
  "QUERY against collection CN with the provider that writes it, as a Result:
   in a fan-out one collection's failure must not fail the others."
  [cn shaped query]
  (try
    (let [route (host/resolve-provider-for-type (if (= cn "hive-mcp-memory") "note" "decision"))]
      (query-route (assoc route :collection-name cn) shaped query))
    (catch Exception e
      (failure :search/route-failed e))))

(defn- fan-out-search
  "QUERY against every collection of COLL-NAMES, merged by distance."
  [coll-names shaped query]
  (->> coll-names
       (mapcat #(hits-or-empty % (fan-out-route % shaped query)))
       (sort-by #(or (:distance %) 999.0))
       (take (:limit query))
       vec))

(m/=> failure [:=> [:cat :keyword :any] SearchResult])
(m/=> search-collection [:=> [:cat chroma/Coll Query] SearchResult])
(m/=> hits-or-empty [:=> [:cat [:maybe :string] SearchResult] [:sequential Hit]])
(m/=> shape-query [:=> [:cat :string :any] :any])
(m/=> query-route [:=> [:cat conn/Route :any Query] SearchResult])
(m/=> fan-out-route [:=> [:cat :string :any Query] SearchResult])
(m/=> fan-out-search [:=> [:cat [:sequential :string] :any Query] [:vector Hit]])

(defn search-similar
  "Search for memory entries similar to the query text.
   Routes to type-specific collection when type is provided.
   Fans out to all collections when type is nil.
   :exclude-tags — seq of tags to exclude via where-document $not_contains.
   :query-template overrides embeddings.query-template (:raw by default).

   With one collection (or none named: no provider configured, so writes land
   in the default collection) the type's own route is searched; with several,
   each is searched with its own provider and the hits merged by distance."
  [query-text & {:keys [limit type project-ids exclude-tags query-template] :or {limit 10}}]
  (host/require-embedding!)
  (let [query      {:limit          limit
                    :where          (build-where-clause {:type type :project-ids project-ids})
                    :where-document (build-where-document-clause exclude-tags)}
        shaped     (shape-query query-text query-template)
        coll-names (r/rescue [nil] (host/type->collection-names type))
        hits       (if (<= (count coll-names) 1)
                     (let [route (r/rescue nil (host/resolve-provider-for-type (or type "note")))]
                       (hits-or-empty (first coll-names) (query-route route shaped query)))
                     (fan-out-search coll-names shaped query))]
    (log/debug "Semantic search for:" (subs query-text 0 (min 50 (count query-text)))
               "found:" (count hits))
    hits))

(m/=> search-similar [:=> [:cat :string KwOpts] [:sequential Hit]])

(defn search-by-id
  "Get a specific entry by ID from Chroma."
  [id]
  (let [coll (conn/get-or-create-collection)
        results (gate/deref-read (chroma/get coll :ids [id] :include #{:documents :metadatas}))]
    (first results)))

(m/=> search-by-id [:=> [:cat :any] [:maybe Hit]])

;; --- Federated Search (default + ingest collections) ---

(defn search-federated
  "Search default memory collection + all ingest collections.
   Merges results, deduplicates by ID, re-ranks by distance (ascending).
   Returns unified result vector matching search-similar output shape."
  [query-text & {:keys [limit type project-ids exclude-tags] :or {limit 10}}]
  (let [default-results (search-similar query-text
                                        :limit limit :type type
                                        :project-ids project-ids
                                        :exclude-tags exclude-tags)
        ingest-results  (when-let [search-fn (host/resolve-ingest-search)]
                          (r/guard Exception nil
                            (let [result (search-fn query-text {:limit limit})]
                              (when (and (map? result) (:ok result))
                                (host/normalize-ingest-results (:ok result))))))
        merged (host/merge-and-rerank default-results ingest-results limit)]
    (log/debug "Federated search:" (count default-results) "default +"
               (count ingest-results) "ingest =" (count merged) "merged")
    merged))

(m/=> search-federated [:=> [:cat :string KwOpts] [:vector Hit]])
