(ns hive-chroma.core-contract-test
  "The chroma.core facade under instrumentation, down both of its routes:
   an active IMemoryStore (the atom-backed stub) and the concrete
   chroma.crud fallback over the in-memory transport."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-chroma.core :as core]
            [hive-chroma.helpers :as h]
            [hive-chroma.test.fixtures :as fixtures]
            [hive-chroma.test.memory-port :as pmem]
            [hive-chroma.test.memory-store :as stub]
            [hive-test.golden :refer [deftest-golden]]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:private contracted
  #{#'core/answer #'core/index-memory-entry! #'core/get-entry-by-id
    #'core/query-entries #'core/update-entry! #'core/delete-entry!
    #'core/find-duplicate #'core/search-similar #'core/cleanup-expired!
    #'core/entries-expiring-soon})

(use-fixtures :each (fixtures/chroma-without-server {:instrument contracted}))

(defn- exercise
  "Drive every facade fn once; answer what a golden can pin (no ids, no clock)."
  []
  (let [content "facade body"
        hash    (h/content-hash content)
        id      (core/index-memory-entry! {:type "note" :content content
                                           :tags ["t"] :content-hash hash
                                           :duration "medium"
                                           :expires (str (.plusDays (java.time.ZonedDateTime/now) 3))})
        got     (core/get-entry-by-id id)
        upd     (core/update-entry! id {:tags ["t" "u"]})
        dup     (core/find-duplicate "note" hash)
        q       (core/query-entries :type "note" :limit 5)
        hits    (core/search-similar "facade" :type "note" :limit 5)
        soon    (core/entries-expiring-soon 7)
        clean   (core/cleanup-expired!)
        del     (core/delete-entry! id)]
    {:answer        (core/answer)
     :id-string?    (string? id)
     :got-content   (:content got)
     :updated-tags  (:tags upd)
     :dup-found?    (= id (:id dup))
     :queried       (mapv :content q)
     :hit?          (boolean (some #(= id (:id %)) hits))
     :expiring      (mapv :content soon)
     :cleanup-count (:count clean)
     :deleted       (if (string? del) :id del)
     :after-delete  (core/get-entry-by-id id)}))

(deftest store-routing-is-observable
  (testing "no active store: the concrete chroma.crud route"
    (is (false? (pmem/store-set?)))))

(deftest-golden facade-without-store
  "test/golden/chroma/core-facade-fallback.edn"
  (exercise))

(deftest-golden facade-through-active-store
  "test/golden/chroma/core-facade-active-store.edn"
  (stub/with-stub-store
    (fn []
      (assert (pmem/store-set?))
      (exercise))))
