(ns hive-chroma.test.memory-port
  "The memory port surface the suite drives a store through, in one alias:
   the hive-spi.memory.ports methods, the hive-spi.memory.registry slot, and
   the capability predicates a host derives from them. hive-spi only."
  (:require [hive-spi.memory.ports :as ports]
            [hive-spi.memory.registry :as registry]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; Methods: delegate per call, so an impl extended after this ns loaded is seen.
(defn connected? [s] (ports/connected? s))
(defn health-check [s] (ports/health-check s))
(defn disconnect! [s] (ports/disconnect! s))
(defn add-entry! [s e] (ports/add-entry! s e))
(defn get-entry [s id] (ports/get-entry s id))
(defn update-entry! [s id u] (ports/update-entry! s id u))
(defn delete-entry! [s id] (ports/delete-entry! s id))
(defn query-entries [s opts] (ports/query-entries s opts))
(defn search-similar [s q opts] (ports/search-similar s q opts))
(defn supports-semantic-search? [s] (ports/supports-semantic-search? s))
(defn store-status [s] (ports/store-status s))
(defn reset-store! [s] (ports/reset-store! s))
(defn get-entries [s ids] (ports/get-entries s ids))

;; Protocols, for satisfies? checks.
(def IMemoryStore ports/IMemoryStore)
(def IMemoryStoreWithAnalytics ports/IMemoryStoreWithAnalytics)
(def IMemoryStoreWithStaleness ports/IMemoryStoreWithStaleness)

;; Capability predicates.
(defn analytics-store? [s] (satisfies? ports/IMemoryStoreWithAnalytics s))
(defn staleness-store? [s] (satisfies? ports/IMemoryStoreWithStaleness s))
(defn batch-store? [s] (satisfies? ports/IMemoryStoreBatch s))

;; The :default store slot.
(defn store-set? [] (registry/store-set?))
(defn get-store [] (registry/get-store))
(defn set-store! [s] (registry/set-store! s))
(defn reset-registry! [] (registry/reset-registry!))
(defn registered-stores [] (registry/registered-stores))
