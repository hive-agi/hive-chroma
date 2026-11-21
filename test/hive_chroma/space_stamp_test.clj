(ns hive-chroma.space-stamp-test
  "Collection provenance: a vector collection is stamped with the embedding
   space it was created in, and every write and search through it is refused
   when the provider now emits another space. Unstamped (legacy) collections
   are admitted, never refused, and `collection-space-audit` lists every
   collection's provenance as data.

   Everything runs over stub ports: the in-memory Chroma transport, the stub
   host published through hive-spi (hive-chroma.test.host) with a mock
   embedder per routed provider, an EDN config and a digest resolver that
   answers from an atom. No live store, no Ollama, no hive-mcp."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-chroma.client :as client]
            [hive-chroma.connection :as conn]
            [hive-chroma.crud :as crud]
            [hive-chroma.gate :as gate]
            [hive-chroma.search :as search]
            [hive-chroma.test.fixtures :as fixtures]
            [hive-chroma.test.host :as host]
            [hive-chroma.stub.transport :as transport]
            [hive-spi.embeddings.space :as space]
            [malli.instrument :as mi]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:private dim 8)
(def ^:private coll-name "hive-mcp-memory-8d")

(def ^:private embedder-config
  {:embedder {:default :stub-8 :routes {} :no-embed-types #{}
              :providers {:stub-8 {:impl :ollama :model "stub-embed" :dimension dim
                                   :max-tokens 8192 :host "http://stub.invalid"}}}})

(def ^:private digest (atom "sha256:aaaa"))

(def ^:private contracted
  #{#'space/space->metadata #'space/metadata->space #'space/admit #'space/audit
    #'conn/get-or-create-named-collection #'conn/get-or-create-collection
    #'conn/collection-names #'conn/collection-space-audit})

(def ^:private ^:dynamic *transport* nil)

(defn- connection-caches []
  {:collection @@#'conn/collection-cache
   :named @@#'conn/named-collection-cache
   :legacy-warned @@#'conn/legacy-warned})

(defn- restore-connection-caches! [caches]
  (reset! @#'conn/collection-cache (:collection caches))
  (reset! @#'conn/named-collection-cache (:named caches))
  (reset! @#'conn/legacy-warned (:legacy-warned caches)))

(defn- digest-port! []
  (host/set-digest-resolver! (fn [_spec] @digest)))

(defn- stub-ports [f]
  (let [prior-t       (client/transport)
        prior-p       (host/get-embedding-provider)
        prior-spec    (host/get-embedding-provider-spec)
        prior-factory (host/get-provider-factory)
        prior-caches  (connection-caches)
        t             (transport/->transport)]
    (client/set-transport! t)
    (host/set-embedding-provider! (fixtures/->MockEmbedder dim))
    (host/set-provider-factory! (fn [spec] (fixtures/->MockEmbedder (:dimension spec))))
    (conn/reset-collection-cache!)
    (reset! digest "sha256:aaaa")
    (digest-port!)
    (mi/instrument! {:filters [(mi/-filter-var contracted)]})
    (try
      (binding [*transport* t]
        (fixtures/with-config embedder-config (f)))
      (finally
        (mi/unstrument! {:filters [(mi/-filter-var contracted)]})
        (host/set-digest-resolver! (fn [_spec] nil))
        (host/set-provider-factory! prior-factory)
        (restore-connection-caches! prior-caches)
        (client/set-transport! prior-t)
        (if prior-p
          (host/set-embedding-provider! prior-p prior-spec)
          (host/reset-embedding-provider!))))))

(use-fixtures :each stub-ports)

(defn- space-of [overrides]
  (space/->space (merge {:provider :ollama :model "stub-embed" :digest "sha256:aaaa"
                         :dimension dim :metric :l2 :query-template-rev "raw"
                         :doc-serializer-rev host/doc-serializer-rev}
                        overrides)))

(defn- stored-metadata [name]
  (get-in @(:state *transport*) [name :metadata]))

(defn- thrown-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

;; =============================================================================
;; Pure stamp
;; =============================================================================

(deftest stamp-round-trips
  (let [s (space-of {})]
    (testing "the stamp is flat scalar metadata carrying the space id"
      (let [md (space/space->metadata s)]
        (is (= (space/space-id s) (:embed-space-id md)))
        (is (= "ollama" (:embed-provider md)))
        (is (every? (some-fn string? int? boolean?) (vals md)))))
    (testing "read back with keyword or string keys it is the same space"
      (is (= s (space/metadata->space (space/space->metadata s))))
      (is (= s (space/metadata->space (update-keys (space/space->metadata s) name)))))
    (testing "unrecorded fields stay unrecorded"
      (let [bare (space-of {:digest nil :query-template-rev nil})]
        (is (not (contains? (space/space->metadata bare) :embed-digest)))
        (is (= bare (space/metadata->space (space/space->metadata bare))))))
    (testing "metadata without a stamp is legacy: no space"
      (is (nil? (space/metadata->space nil)))
      (is (nil? (space/metadata->space {:dimension 8 :created-by "hive-mcp"}))))))

(deftest admit-verdicts
  (let [s (space-of {})]
    (is (= :stamped (:verdict (space/admit coll-name s (space/space->metadata s)))))
    (is (= :legacy (:verdict (space/admit coll-name s {:dimension 8}))))
    (is (= :legacy (:verdict (space/admit coll-name s nil))))
    (let [data (thrown-data #(space/admit coll-name s
                                          (space/space->metadata (space-of {:digest "sha256:bbbb"}))))]
      (is (= :embedding-space-mismatch (:error data)))
      (is (= coll-name (:collection data)))
      (is (= [:digest] (:differs-on data)))
      (is (= #{:isolate :re-embed} (:remedy data))))
    (testing "a serializer revision change is a different space too"
      (is (= [:doc-serializer-rev]
             (:differs-on (thrown-data #(space/admit coll-name s
                                                     (space/space->metadata
                                                      (space-of {:doc-serializer-rev "old/0"}))))))))))

;; =============================================================================
;; Connection: stamp on create, check on open
;; =============================================================================

(deftest collection-is-stamped-on-create-and-checked-on-open
  (let [s (space-of {})]
    (conn/get-or-create-named-collection coll-name dim s)
    (is (= s (space/metadata->space (stored-metadata coll-name)))
        "the new collection carries its space")
    (is (some? (conn/get-or-create-named-collection coll-name dim s))
        "the same space reopens it")
    (conn/reset-collection-cache!)
    (is (= :embedding-space-mismatch
           (:error (thrown-data #(conn/get-or-create-named-collection
                                  coll-name dim (space-of {:model "other-embed"})))))
        "another space is refused, also after the cache is dropped")
    (is (some? (conn/get-or-create-named-collection coll-name dim))
        "the unguarded arity is unchanged")))

(deftest legacy-collection-is-admitted-not-refused
  (gate/deref-write (client/create-collection coll-name :metadata {:dimension dim}))
  (let [coll (conn/get-or-create-named-collection coll-name dim (space-of {}))]
    (is (= coll-name (:name coll)))
    (is (nil? (space/metadata->space (stored-metadata coll-name)))
        "admitting a legacy collection never rewrites it")))

;; =============================================================================
;; Write and search paths
;; =============================================================================

(deftest write-path-refuses-a-drifted-model
  (let [id (crud/index-memory-entry! {:id "e1" :type "note" :content "alpha"})]
    (is (= "e1" id))
    (is (= "sha256:aaaa" (:digest (space/metadata->space (stored-metadata coll-name))))
        "the first write stamps the collection with the served digest")
    (is (= 1 (transport/record-count *transport*)))
    (testing "the same tag re-pulled with new weights refuses to write"
      (reset! digest "sha256:bbbb")
      (conn/reset-collection-cache!)
      (let [data (thrown-data #(crud/index-memory-entry! {:id "e2" :type "note" :content "beta"}))]
        (is (= :embedding-space-mismatch (:error data)))
        (is (= [:digest] (:differs-on data)))
        (is (= coll-name (:collection data))))
      (is (= 1 (transport/record-count *transport*)) "nothing was written"))))

(deftest search-path-refuses-a-drifted-model
  (crud/index-memory-entry! {:id "e1" :type "note" :content "alpha"})
  (is (= ["e1"] (mapv :id (search/search-similar "alpha" :type "note" :limit 3))))
  (reset! digest "sha256:bbbb")
  (conn/reset-collection-cache!)
  (is (= :embedding-space-mismatch
         (:error (thrown-data #(search/search-similar "alpha" :type "note" :limit 3))))
      "a query embedded in another space is refused, never answered"))

(deftest legacy-collection-keeps-serving-writes-and-searches
  (gate/deref-write (client/create-collection coll-name :metadata {:dimension dim}))
  (is (= "e1" (crud/index-memory-entry! {:id "e1" :type "note" :content "alpha"})))
  (is (= ["e1"] (mapv :id (search/search-similar "alpha" :type "note" :limit 3)))))

;; =============================================================================
;; Default (unnamed) collection and the batch path
;; =============================================================================

(def ^:private global-spec
  {:impl :ollama :model "stub-embed" :host "http://stub.invalid"})

(defn- default-name [] (:collection-name (conn/get-config)))

(defn- name-global-provider! []
  (host/set-embedding-provider! (fixtures/->MockEmbedder dim) global-spec))

(def ^:private batch
  [{:id "b1" :type "note" :content "alpha"} {:id "b2" :type "note" :content "beta"}])

(deftest default-collection-is-stamped
  (testing "the unguarded accessor stamps the collection it creates"
    (name-global-provider!)
    (is (= (default-name) (:name (conn/get-or-create-collection))))
    (is (= (space-of {}) (space/metadata->space (stored-metadata (default-name))))))
  (testing "the guarded arity admits the stamped space and refuses another"
    (is (some? (conn/get-or-create-collection (space-of {}))))
    (let [data (thrown-data #(conn/get-or-create-collection (space-of {:model "other-embed"})))]
      (is (= :embedding-space-mismatch (:error data)))
      (is (= (default-name) (:collection data))))))

(deftest default-collection-of-an-unnamed-provider-stays-unstamped
  (is (= (default-name) (:name (conn/get-or-create-collection))))
  (is (nil? (space/metadata->space (stored-metadata (default-name))))))

(deftest batch-index-stamps-on-create
  (name-global-provider!)
  (is (= ["b1" "b2"] (crud/index-memory-entries! batch)))
  (is (= (space-of {}) (space/metadata->space (stored-metadata (default-name))))
      "the batch that creates the default collection stamps it")
  (is (= 2 (transport/record-count *transport*))))

(deftest batch-index-refuses-a-mismatched-space
  (name-global-provider!)
  (crud/index-memory-entries! batch)
  (reset! digest "sha256:bbbb")
  (testing "refused through the cached collection and after the cache is dropped"
    (doseq [drop-cache? [false true]]
      (when drop-cache? (conn/reset-collection-cache!))
      (let [data (thrown-data #(crud/index-memory-entries! [{:id "b3" :type "note" :content "gamma"}]))]
        (is (= :embedding-space-mismatch (:error data)))
        (is (= [:digest] (:differs-on data)))
        (is (= (default-name) (:collection data))))))
  (is (= 2 (transport/record-count *transport*)) "nothing was written"))

(deftest batch-index-admits-a-legacy-default-collection
  (gate/deref-write (client/create-collection (default-name) :metadata {:dimension dim}))
  (name-global-provider!)
  (is (= ["b1" "b2"] (crud/index-memory-entries! batch)))
  (is (nil? (space/metadata->space (stored-metadata (default-name))))
      "admitting a legacy collection never rewrites it"))

(deftest default-collection-stays-unstamped-when-the-digest-is-unresolved
  (name-global-provider!)
  (host/set-digest-resolver! (fn [_spec] nil))
  (conn/reset-collection-cache!)
  (conn/get-or-create-collection)
  (is (nil? (space/metadata->space (stored-metadata (default-name))))
      "an Ollama model without its served digest names no space, so no stamp"))

(deftest read-path-does-not-stamp
  (testing "real zero-arity read paths do not stamp an existing collection"
    (name-global-provider!)
    (host/set-digest-resolver! (fn [_spec] "sha256:test-read-digest"))
    (conn/reset-collection-cache!)
    ;; Raw creation leaves a legacy collection. Its metadata is the witness:
    ;; the digest below is resolvable, but no read may write it.
    (gate/deref-write (client/create-collection (default-name) :metadata {:dimension dim}))
    (is (nil? (space/metadata->space (stored-metadata (default-name)))))
    (is (true? (conn/chroma-available?)))
    (is (nil? (crud/get-entry-by-id "absent")))
    (is (= {:count 0 :types {}} (crud/collection-stats)))
    (is (nil? (space/metadata->space (stored-metadata (default-name))))
        "chroma-available?, get-entry-by-id and collection-stats never stamp")))

;; =============================================================================
;; Legacy listing: audit every collection without dying on the first mismatch
;; =============================================================================

(deftest audit-lists-every-collection-that-exists
  (name-global-provider!)
  (gate/deref-write (client/create-collection "hive-mcp-memory-legacy" :metadata {:dimension dim}))
  (conn/reset-collection-cache!)
  (conn/get-or-create-named-collection coll-name dim (space-of {:digest "sha256:bbbb"}))
  (let [rows    (conn/collection-space-audit {:names ["hive-mcp-memory-legacy"]
                                              :space (space-of {})})
        by-name (into {} (map (juxt :collection identity)) rows)]
    (testing "every existing collection is visited, and none is created"
      (is (= #{"hive-mcp-memory-legacy" coll-name} (set (keys by-name))))
      (is (= #{"hive-mcp-memory-legacy" coll-name} (set (keys @(:state *transport*))))
          "the audit asks only for names that exist"))
    (testing "an unstamped collection is legacy with a remedy, not a throw"
      (is (= :legacy (get-in by-name ["hive-mcp-memory-legacy" :verdict])))
      (is (= space/unknown-space-id (get-in by-name ["hive-mcp-memory-legacy" :space-id])))
      (is (= #{:isolate :re-embed} (get-in by-name ["hive-mcp-memory-legacy" :remedy]))))
    (testing "a collection stamped with another space is divergent, not an exception"
      (is (= :diverged (get-in by-name [coll-name :verdict])))
      (is (= [:digest] (get-in by-name [coll-name :differing-keys])))
      (is (= (space/space-id (space-of {})) (get-in by-name [coll-name :expected-id]))))))

(deftest audit-without-an-expected-space-never-names-one
  (name-global-provider!)
  (conn/get-or-create-collection (space-of {}))
  (let [rows (conn/collection-space-audit)]
    (is (seq rows) "the default collection exists and is listed")
    (is (every? #(not (contains? % :expected-id)) rows)
        "no expected space was given, so no row claims one")
    (is (every? #(contains? #{:stamped :legacy} (:verdict %)) rows))))

(deftest collection-names-come-from-config-only
  (is (= ["hive-mcp-memory" coll-name "extra"] (conn/collection-names ["extra"])))
  (is (empty? @(:state *transport*)) "naming collections opens nothing"))
