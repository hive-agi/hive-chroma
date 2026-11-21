(ns hive-chroma.stages-trifecta-test
  "Trifectas (golden + property + mutation) for the pure stages the write,
   read and repair paths are built from, and the embed-failover stage over an
   injected provider. No server, no rebinding: the stub host and in-memory
   transport are installed by the fixture and restored after it."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-chroma.client :as client]
            [hive-chroma.connection :as conn]
            [hive-chroma.crud :as crud]
            [hive-chroma.helpers :as h]
            [hive-chroma.host :as host]
            [hive-chroma.maintenance :as maintenance]
            [hive-chroma.search :as search]
            [hive-chroma.stub.transport :as transport]
            [hive-chroma.test.fixtures :as fixtures]
            [hive-chroma.test.host :as stub-host]
            [hive-schemas.test :refer [deftrifecta-from-schema]]
            [hive-spi.embeddings.ports :as emb]
            [hive-test.trifecta :refer [deftrifecta]]
            [malli.generator :as mg]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defn- stub-ports [f]
  (let [prior-t (client/transport)
        prior-p (stub-host/get-embedding-provider)]
    (client/set-transport! (transport/->transport))
    (stub-host/set-embedding-provider! (fixtures/->MockEmbedder 8))
    (conn/reset-collection-cache!)
    (try
      (fixtures/with-config {:embedder {:no-embed-types #{} :routes {}}} (f))
      (finally
        (conn/reset-collection-cache!)
        (client/set-transport! prior-t)
        (if prior-p
          (stub-host/set-embedding-provider! prior-p)
          (stub-host/reset-embedding-provider!))))))

(use-fixtures :each stub-ports)

;; =============================================================================
;; crud/query-where: filters -> Chroma where clause
;; =============================================================================

(def ^:private Filters
  [:map
   [:type {:optional true} [:maybe [:enum "note" "decision"]]]
   [:project-id {:optional true} [:maybe [:enum "p1" "global"]]]
   [:project-ids {:optional true} [:maybe [:vector {:gen/max 2} [:enum "p1" "p2"]]]]
   [:tags {:optional true} [:maybe [:vector {:gen/max 2} [:enum "a" "b"]]]]
   [:exclude-tags {:optional true} [:maybe [:vector {:gen/max 2} [:enum "x" "y"]]]]])

(defn- where-terms
  "Every {field condition} term WHERE constrains, flattened out of an $and."
  [where]
  (if-let [terms (:$and where)] (set terms) (set (map (fn [[k v]] {k v}) where))))

(deftrifecta-from-schema query-where
  hive-chroma.crud/query-where
  {:in Filters
   :out crud/Where
   :rel (fn [{:keys [type project-id project-ids tags exclude-tags]} out]
          (let [terms (where-terms out)]
            (and (= (nil? out) (and (nil? type) (nil? project-id) (nil? project-ids)
                                    (empty? tags) (empty? exclude-tags)))
                 (or (nil? type) (contains? terms {:type type}))
                 (if project-ids
                   (contains? terms {:project-id {:$in (vec project-ids)}})
                   (or (nil? project-id) (contains? terms {:project-id project-id})))
                 (every? #(contains? terms {:tags {:$contains %}}) tags)
                 (every? #(contains? terms {:tags {:$not_contains %}}) exclude-tags))))
   :golden-path "test/golden/chroma/query-where.edn"
   :num-tests 200})

;; =============================================================================
;; maintenance/repair-updates: one entry's duration/expires fix
;; =============================================================================

(def ^:private now "2026-01-01T00:00:00Z")

(defn repair-at
  "repair-updates with NOW given as an ISO string, so the golden snapshot of
   its inputs stays readable EDN."
  [entry now-iso]
  (maintenance/repair-updates entry (java.time.ZonedDateTime/parse now-iso)))

(def ^:private RepairIn
  [:cat
   [:map
    [:id [:enum "e1" "e2"]]
    [:metadata [:map
                [:duration [:maybe [:enum "ephemeral" "short" "medium" "long" "permanent"
                                    "session" "long-term" "project"]]]
                [:expires [:enum "" "2027-01-01T00:00:00Z"]]]]]
   [:= now]])

(deftrifecta-from-schema repair-updates
  hive-chroma.stages-trifecta-test/repair-at
  {:in RepairIn
   :out [:maybe maintenance/Repair]
   :rel (fn [[entry at] out]
          (let [raw       (get-in entry [:metadata :duration])
                canonical (h/normalize-duration raw)
                renamed?  (not= raw canonical)
                days      ({"ephemeral" 1 "short" 7 "medium" 30 "long" 90} canonical)
                expire?   (boolean (and days (or renamed? (str/blank? (get-in entry [:metadata :expires])))))]
            (and (= (nil? out) (not (or renamed? expire?)))
                 (= (when renamed? canonical) (:duration out))
                 (= (when expire? (str (.plusDays (java.time.ZonedDateTime/parse at) days)))
                    (:expires out)))))
   :golden-path "test/golden/chroma/repair-updates.edn"
   :num-tests 200})

;; =============================================================================
;; crud/entry->metadata: the promote stage of index-memory-entry!
;; =============================================================================

(def ^:private MetadataIn
  [:cat
   [:map
    [:content [:or [:enum "body" ""] [:map [:k [:enum 1 2]]]]]
    [:type {:optional true} [:enum "note" "decision"]]
    [:tags {:optional true} [:maybe [:vector {:gen/max 3} [:enum "a" "b" "c"]]]]
    [:kg-outgoing {:optional true} [:maybe [:vector {:gen/max 2} [:enum "n1" "n2"]]]]
    [:created {:optional true} [:enum "2025-01-01T00:00:00Z"]]
    [:staleness-source {:optional true} [:maybe [:enum :transitive :direct]]]]
   [:= "2026-01-01T00:00:00Z"]])

(deftrifecta-from-schema entry->metadata
  hive-chroma.crud/entry->metadata
  {:in MetadataIn
   :out crud/Metadata
   :rel (fn [[entry at] out]
          (and (every? #(contains? out %) (keys h/metadata-defaults))
               (= (h/serialize-content (:content entry)) (:content out))
               (= (or (h/join-tags (:tags entry)) "") (:tags out))
               (= (or (h/join-tags (:kg-outgoing entry)) "") (:kg-outgoing out))
               (= (or (:created entry) at) (:created out))
               (= at (:updated out))
               (= (or (:type entry) "note") (:type out))
               (= (some-> (:staleness-source entry) name) (:staleness-source out))
               (not (contains? out :embed-text))))
   :golden-path "test/golden/chroma/entry-metadata.edn"
   :num-tests 200})

;; =============================================================================
;; helpers/join-tags: the tag codec every metadata write goes through
;; =============================================================================

(deftrifecta join-tags
  hive-chroma.helpers/join-tags
  {:golden-path "test/golden/chroma/join-tags.edn"
   :cases {:nil nil :empty [] :single ["a"] :many ["a" "b" "c"]}
   :gen (mg/generator [:maybe [:vector {:gen/max 5} [:string {:min 1 :gen/max 6}]]])
   :pred #(or (nil? %) (string? %))
   :num-tests 100
   :mutations [["always-nil" (constantly nil)]
               ["space-joined" (fn [tags] (when (seq tags) (str/join " " tags)))]]
   :assert (fn []
             (is (= "a,b" (h/join-tags ["a" "b"])))
             (is (nil? (h/join-tags [])))
             (is (= ["a" "b"] (h/split-tags (h/join-tags ["a" "b"])))))})

;; =============================================================================
;; search/hits-or-empty: which search failures drop a collection
;; =============================================================================

(deftest hits-or-empty-keeps-hits-and-drops-failures
  (let [hits-or-empty #'search/hits-or-empty]
    (is (= [{:id "a"}] (hits-or-empty "c" {:ok [{:id "a"}]})))
    (is (= [] (hits-or-empty "c" {:error :search/collection-failed :message "down"})))
    (testing "a space mismatch is a refusal: dropped too, never thrown"
      (is (= [] (hits-or-empty "c" {:error :search/route-failed :message "mismatch"
                                    :data {:error :embedding-space-mismatch}}))))))

;; =============================================================================
;; crud/embed-with-failover over an injected provider
;; =============================================================================

(defrecord FailingEmbedder [dim]
  emb/EmbeddingProvider
  (embed-text [_ _] (throw (ex-info "provider down" {:error :down})))
  (embed-batch [_ _] (throw (ex-info "provider down" {:error :down})))
  (embedding-dimension [_] dim))

(deftest embed-with-failover-embeds-with-the-route-provider
  (let [v (crud/embed-with-failover "e1" {:provider (fixtures/->MockEmbedder 8)
                                          :dimension 8 :provider-key :mock} "hello")]
    (is (= 8 (count v)))
    (is (every? number? v))))

(deftest embed-with-failover-falls-back-to-the-global-provider
  (is (= 8 (count (crud/embed-with-failover "e1" nil "hello")))))

(deftest embed-with-failover-rethrows-when-no-sibling-can-embed
  (let [e (try (crud/embed-with-failover "e1" {:provider (->FailingEmbedder 8)
                                               :dimension 8 :provider-key :failing} "hello")
               nil
               (catch clojure.lang.ExceptionInfo e e))]
    (is (some? e) "a write must never silently skip its vector")
    (is (= {:error :down} (ex-data e)) "the provider's own failure is rethrown unchanged")))

(deftest entry-embedding-stores-a-placeholder-for-no-embed-types
  (let [entry-embedding #'crud/entry-embedding]
    (is (= (vec (repeat 4 0.0)) (entry-embedding "e1" true {:dimension 4} "ignored")))
    (is (= 768 (count (entry-embedding "e1" true nil "ignored"))))))

;; =============================================================================
;; host: the soft-resolved embedding seams (12, 6 and 5 prod callers)
;; =============================================================================

(deftest host-embedding-seams-follow-the-published-host
  (testing "with a provider published, the seams answer it"
    (is (true? (host/embedding-configured?)))
    (is (some? (host/get-embedding-provider)))
    (is (nil? (host/require-embedding!))))
  (testing "with none, the guard throws and the predicates answer false/nil"
    (stub-host/reset-embedding-provider!)
    (is (false? (host/embedding-configured?)))
    (is (nil? (host/get-embedding-provider)))
    (is (= :no-embedding-provider
           (:type (ex-data (try (host/require-embedding!) nil
                                (catch clojure.lang.ExceptionInfo e e))))))))

(deftest consumed-capabilities-names-every-seam
  (let [caps (host/consumed-capabilities)]
    (is (contains? caps :embedding/require!))
    (is (contains? caps :routing/sibling-failover))
    (is (every? keyword? caps))))
