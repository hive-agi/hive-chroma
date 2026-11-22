(ns hive-chroma.seam-trifecta-test
  "Trifectas (golden + property + mutation) for the high fan-in seams:
   host/require-embedding! (12 prod callers), host/get-embedding-provider (6),
   connection/metric and helpers/join-tags.

   A host seam's input is the HOST'S STATE, not an argument, so each seam is
   driven through a probe that takes that state as a value (the host absent,
   published with no provider, or published with a provider of dimension n),
   publishes it through hive-spi exactly as a host does, and reads the real
   seam. The probe carries the golden and property facets. A second mutation
   suite mutates the REAL seam var, so the facets are shown to catch a broken
   seam, not only a broken probe. No server, no with-redefs: the stub host,
   the in-memory transport and the host-services registry are restored after
   every test."
  (:require [clojure.string :as str]
            [clojure.test :refer [is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [hive-chroma.client :as client]
            [hive-chroma.connection :as conn]
            [hive-chroma.helpers :as h]
            [hive-chroma.host :as host]
            [hive-chroma.stub.transport :as transport]
            [hive-chroma.test.fixtures :as fixtures]
            [hive-chroma.test.host :as stub-host]
            [hive-schemas.test :refer [deftrifecta-from-schema]]
            [hive-spi.embeddings.ports :as emb]
            [hive-spi.vector.host-services :as hs]
            [hive-test.mutation :refer [deftest-mutations]]
            [hive-test.trifecta :refer [deftrifecta]]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defn- with-restored-host-services
  "Run G, then put back the host-services registry exactly as it was: a probe
   that models an absent host unregisters every capability."
  [g]
  (let [prior (hs/registered)]
    (try (g)
         (finally
           (hs/unregister-services!)
           (hs/register-services! prior)))))

(def ^:private routed-config
  "Two routed providers, so the metric probe stamps named collections as well
   as the default one."
  {:embedder {:no-embed-types #{} :routes {}
              :providers {:stub-8  {:impl :openai :model "stub-8" :dimension 8}
                          :stub-16 {:impl :openai :model "stub-16" :dimension 16}}}})

(use-fixtures :each
  (fixtures/chroma-without-server {:embedder (fixtures/->MockEmbedder 8)
                                   :config   routed-config
                                   :wrap     with-restored-host-services}))

;; =============================================================================
;; Host state as a value
;; =============================================================================

(def ^:private host-states
  "Every host state a seam must answer: no host at all, a host with no
   provider, and a host with a provider of each dimension."
  [:host-absent :none 8 16 384])

(defn- publish!
  "Make HOST-STATE the state the seams see, through hive-spi publication."
  [host-state]
  (case host-state
    :host-absent (hs/unregister-services!)
    :none        (do (stub-host/install!) (stub-host/reset-embedding-provider!))
    (do (stub-host/install!)
        (stub-host/set-embedding-provider! (fixtures/->MockEmbedder host-state)))))

;; =============================================================================
;; host/get-embedding-provider
;; =============================================================================

(defn provider-seen
  "Probe: publish HOST-STATE, then the dimension of the provider the
   get-embedding-provider seam answers, or nil when it answers none."
  [host-state]
  (publish! host-state)
  (some-> (host/get-embedding-provider) emb/embedding-dimension))

(defn- provider-oracle
  "What the seam must answer: the published provider, else nothing."
  [host-state]
  (when (int? host-state) host-state))

(deftrifecta get-embedding-provider
  hive-chroma.seam-trifecta-test/provider-seen
  {:golden-path   "test/golden/chroma/seam-get-embedding-provider.edn"
   :cases         {:host-absent :host-absent :none :none :d8 8 :d384 384}
   :gen           (gen/elements host-states)
   :property-type :oracle
   :oracle        provider-oracle
   :num-tests     60
   :mutations     [["host-ignored" (fn [_] nil)]
                   ["stale-provider" (fn [_] 384)]]})

(deftest-mutations get-embedding-provider-seam-mutations
  hive-chroma.host/get-embedding-provider
  [["answers-nothing" (constantly nil)]
   ["frozen-provider" (let [p (fixtures/->MockEmbedder 3)] (constantly p))]]
  (fn []
    (doseq [s host-states]
      (is (= (provider-oracle s) (provider-seen s)) (str "host state " s)))))

;; =============================================================================
;; host/require-embedding!
;; =============================================================================

(defn guard-verdict
  "Probe: publish HOST-STATE, then what the require-embedding! guard does:
   :ok when it passes, else the :type of the refusal it throws."
  [host-state]
  (publish! host-state)
  (try (host/require-embedding!)
       :ok
       (catch clojure.lang.ExceptionInfo e
         (or (:type (ex-data e)) :untyped-refusal))))

(defn- guard-oracle
  "A write or search must never run without a provider: refuse, typed."
  [host-state]
  (if (int? host-state) :ok :no-embedding-provider))

(deftrifecta require-embedding
  hive-chroma.seam-trifecta-test/guard-verdict
  {:golden-path   "test/golden/chroma/seam-require-embedding.edn"
   :cases         {:host-absent :host-absent :none :none :d8 8}
   :gen           (gen/elements host-states)
   :property-type :oracle
   :oracle        guard-oracle
   :num-tests     60
   :mutations     [["never-refuses" (fn [_] :ok)]
                   ["always-refuses" (fn [_] :no-embedding-provider)]]})

(deftest-mutations require-embedding-seam-mutations
  hive-chroma.host/require-embedding!
  [["never-refuses" (constantly nil)]
   ["always-refuses" (fn [] (throw (ex-info "refused" {:type :no-embedding-provider})))]
   ["untyped-refusal" (fn [] (throw (ex-info "refused" {})))]]
  (fn []
    (doseq [s host-states]
      (is (= (guard-oracle s) (guard-verdict s)) (str "host state " s)))))

;; =============================================================================
;; connection/metric
;; =============================================================================

(def ^:private routes
  "Route keys -> the route collection-for-route is handed (nil = default)."
  {:default   nil
   :routed-8  {:collection-name "hive-mcp-memory-8d" :dimension 8 :provider-key :stub-8}
   :routed-16 {:collection-name "hive-mcp-memory-16d" :dimension 16 :provider-key :stub-16}})

(defn stamped-metric
  "Probe: open ROUTE-KEY's collection on a fresh transport and answer the
   distance metric the collection was stamped with."
  [route-key]
  (client/set-transport! (transport/->transport))
  (conn/reset-collection-cache!)
  (stub-host/set-embedding-provider! (fixtures/->MockEmbedder 8) {:impl :openai :model "global"})
  (let [coll (conn/collection-for-route (get routes route-key))]
    (get-in @(:state (client/transport)) [(:name coll) :metadata :embed-metric])))

(def ^:private chroma-default-distance
  "What Chroma scores a collection with when no hnsw:space is given, and what
   the in-memory transport ranks by: the stamp must name it, or a space
   recorded on a collection would lie about how its vectors are compared."
  "l2")

(deftrifecta metric
  hive-chroma.seam-trifecta-test/stamped-metric
  {:golden-path   "test/golden/chroma/seam-metric.edn"
   :cases         {:default :default :routed-8 :routed-8 :routed-16 :routed-16}
   :gen           (gen/elements (keys routes))
   :property-type :oracle
   :oracle        (constantly chroma-default-distance)
   :num-tests     30
   :mutations     [["unstamped" (fn [_] nil)]
                   ["cosine" (fn [_] "cosine")]]})

(deftest-mutations metric-seam-mutations
  hive-chroma.connection/metric
  [["cosine" :cosine]
   ["inner-product" :ip]
   ["unset" nil]]
  (fn []
    (doseq [r (keys routes)]
      (is (= chroma-default-distance (stamped-metric r)) (str "route " r)))))

;; =============================================================================
;; helpers/join-tags: schema trifecta, split-tags as its inverse
;; =============================================================================

(def ^:private Tags
  "Tag vectors as the write path sees them: tag text never holds the comma
   the codec joins on."
  [:maybe [:vector {:gen/max 5} [:enum "a" "b" "scope:x" "two words" "agent:q2"]]])

(deftrifecta-from-schema join-tags-codec
  hive-chroma.helpers/join-tags
  {:in  Tags
   :out [:maybe [:string {:min 1}]]
   :rel (fn [tags out]
          (and (= (nil? out) (empty? tags))
               (= (seq tags) (seq (h/split-tags out)))
               (or (nil? out) (= (dec (count tags)) (count (re-seq #"," out))))
               (or (nil? out) (not (str/includes? out ", ")))))
   :golden-path "test/golden/chroma/seam-join-tags.edn"
   :num-tests 200})
