(ns hive-chroma.test.fixtures
  "Test doubles for hive-chroma: a deterministic embedding provider and a
   config binding, both injected through the host's own ports (no
   with-redefs). Ported from hive-mcp.test-fixtures and
   hive-mcp.config.test-support, which live in hive-mcp's test tree and so
   are not on this repo's classpath."
  (:require [hive-mcp.config.core :as config]
            [hive-mcp.config.source :as src]
            [hive-spi.embeddings.ports :as emb]))

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

(defn config-source
  "A mutable in-memory config source seeded with `config`."
  [config]
  (src/atom-source (atom config) config))

(defn with-config*
  "Run `f` with `config` bound as the effective host config."
  [config f]
  (binding [config/*config-source* (config-source config)]
    (f)))

(defmacro with-config
  "Bind `config` as the effective host config for the body."
  [config & body]
  `(with-config* ~config (fn [] ~@body)))

(defmacro with-no-config
  "Bind the empty source: every config lookup misses."
  [& body]
  `(binding [config/*config-source* (src/empty-source)]
     ~@body))
