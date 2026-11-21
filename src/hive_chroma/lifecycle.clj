(ns hive-chroma.lifecycle
  "IShutdownHook for the Chroma backend. Priority 210 (client band).

   Ported from the hive-chroma-client stub. On shutdown it disconnects the
   Chroma store registered in the hive-spi memory registry (under :chroma,
   else :default when that is a ChromaMemoryStore). It depends only on the
   hive-spi lifecycle and memory leaves, never on hive-mcp.

   Registration is explicit (`install!`, called by the addon's initialize!),
   not a load-time side effect."
  ;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
  ;;
  ;; SPDX-License-Identifier: AGPL-3.0-or-later
  (:require [hive-chroma.store :as store]
            [hive-spi.lifecycle.ports :as lifecycle]
            [hive-spi.lifecycle.registry :as reg]
            [hive-spi.memory.ports :as mem-ports]
            [hive-spi.memory.registry :as mem-reg]
            [malli.core :as m]
            [hive-spi.log.ports :as log]))

(defn chroma-store
  "The Chroma store this process registered, or nil."
  []
  (let [stores (mem-reg/registered-stores)]
    (or (:chroma stores)
        (let [d (:default stores)]
          (when (store/chroma-store? d) d)))))

(defrecord ChromaShutdown []
  lifecycle/IShutdownHook
  (shutdown-priority [_] 210)
  (shutdown-name     [_] "chroma/close")
  (shutdown!         [_ _ctx]
    (try
      (if-let [st (chroma-store)]
        (do (mem-ports/disconnect! st)
            (log/info "chroma shutdown complete"))
        (log/debug "chroma store not registered; skipping"))
      (catch Throwable t
        (log/error "chroma close failed:" (ex-message t))))))

(defn install!
  "Register the shutdown hook. Idempotent: the registry is keyed by name."
  []
  (reg/register-shutdown! "chroma/close" (->ChromaShutdown)))

(m/=> chroma-store [:=> [:cat] :any])
(m/=> install! [:=> [:cat] :any])
