(ns hive-chroma.test.fixtures
  "Test doubles and fixtures for hive-chroma, shared by every suite:

     - a deterministic embedding provider (MockEmbedder),
     - a config binding (with-config, with-no-config),
     - the `chroma-without-server` fixture factory: a fresh in-memory
       transport, an injected embedder and a static config per test, with
       optional malli instrumentation of the vars a suite contracts,
     - a snapshot/restore pair for the connection's collection caches.

   Everything is injected through the stub host (hive-chroma.test.host),
   which is published through hive-spi, and through the transport seam. No
   with-redefs, no hive-mcp."
  (:require [hive-chroma.client :as client]
            [hive-chroma.connection :as conn]
            [hive-chroma.stub.transport :as transport]
            [hive-chroma.test.host :as host]
            [hive-spi.embeddings.ports :as emb]
            [malli.instrument :as mi]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defrecord MockEmbedder [dimension]
  emb/EmbeddingProvider
  (embed-text [_ text]
    (let [h (hash text)]
      (vec (for [i (range dimension)]
             (-> (bit-xor h i)
                 (mod 1000)
                 (/ 1000.0)
                 (* 2)
                 (- 1))))))
  (embed-batch [this texts]
    (mapv #(emb/embed-text this %) texts))
  (embedding-dimension [_] dimension))

(defn ->MockEmbedder
  "A deterministic hash embedder (tests only)."
  ([] (->MockEmbedder 384))
  ([dimension] (MockEmbedder. dimension)))

(defn with-config*
  "Run `f` with `config` bound as the effective host config."
  [config f]
  (binding [host/*config* config]
    (f)))

(defmacro with-config
  "Bind `config` as the effective host config for the body."
  [config & body]
  `(with-config* ~config (fn [] ~@body)))

(defmacro with-no-config
  "Bind the empty config: every config lookup misses."
  [& body]
  `(binding [host/*config* {}]
     ~@body))

;; =============================================================================
;; Connection caches: snapshot and restore
;; =============================================================================

(defn connection-caches
  "Snapshot the connection's three process-wide caches (default collection,
   named collections, legacy-warned names) as a value."
  []
  {:collection    @@#'conn/collection-cache
   :named         @@#'conn/named-collection-cache
   :legacy-warned @@#'conn/legacy-warned})

(defn restore-connection-caches!
  "Put back a snapshot taken by `connection-caches`."
  [caches]
  (reset! @#'conn/collection-cache (:collection caches))
  (reset! @#'conn/named-collection-cache (:named caches))
  (reset! @#'conn/legacy-warned (:legacy-warned caches))
  nil)

;; =============================================================================
;; chroma-without-server: the per-test port fixture
;; =============================================================================

(def default-config
  "The static host config most suites run under: no routed providers, so
   every type falls back to the one global provider and its default
   collection."
  {:embedder {:no-embed-types #{} :routes {}}})

(defn- host-snapshot
  "Everything the fixture swaps, as a value, so it can be put back."
  []
  {:transport (client/transport)
   :provider  (host/get-embedding-provider)
   :spec      (host/get-embedding-provider-spec)
   :config    (select-keys (conn/get-config) [:host :port :collection-name])
   :caches    (connection-caches)})

(defn- restore-host!
  "Put back a `host-snapshot`."
  [{:keys [transport provider spec config caches]}]
  ;; configure! is a process-wide merge (an addon's initialize! calls it):
  ;; put the prior endpoint back or every later suite sees this test's host.
  ;; It runs before the prior transport returns, so it never reaches a vendor.
  (when (not= config (select-keys (conn/get-config) (keys config)))
    (conn/configure! config))
  (restore-connection-caches! caches)
  (client/set-transport! transport)
  (if provider
    (host/set-embedding-provider! provider spec)
    (host/reset-embedding-provider!)))

(defn chroma-without-server
  "A clojure.test fixture: the Chroma adapter with no server.

   Per test it installs a fresh in-memory transport and the `:embedder`
   provider, drops the collection caches, instruments the `:instrument` vars
   and runs the test under `:config`. Afterwards it puts back the prior
   transport, provider (with its spec), connection config and caches.

   opts:
     :embedder   EmbeddingProvider to publish    (default (->MockEmbedder 16))
     :instrument set of vars to malli-instrument (default none)
     :config     host config for the test        (default `default-config`)
     :wrap       (fn [g]) running the zero-arg test thunk g inside an extra
                 isolation scope, e.g. a restored store registry
                 (default: calls g)"
  ([] (chroma-without-server {}))
  ([{:keys [embedder instrument config wrap]
     :or   {config default-config wrap (fn [g] (g))}}]
   (let [embedder (or embedder (->MockEmbedder 16))
         filters  (when (seq instrument) {:filters [(mi/-filter-var (set instrument))]})]
     (fn [f]
       (let [prior (host-snapshot)]
         (client/set-transport! (transport/->transport))
         (host/set-embedding-provider! embedder)
         (conn/reset-collection-cache!)
         (when filters (mi/instrument! filters))
         (try
           (wrap (fn [] (with-config* config f)))
           (finally
             (when filters (mi/unstrument! filters))
             (restore-host! prior))))))))
