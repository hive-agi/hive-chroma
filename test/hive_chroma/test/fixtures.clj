(ns hive-chroma.test.fixtures
  "Test doubles for hive-chroma: a deterministic embedding provider and a
   config binding, both injected through the stub host
   (hive-chroma.test.host), which is published through hive-spi. No
   with-redefs, no hive-mcp."
  (:require [hive-chroma.test.host :as host]
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
