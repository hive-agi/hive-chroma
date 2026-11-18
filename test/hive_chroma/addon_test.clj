(ns hive-chroma.addon-test
  "The IAddon over INJECTED ports: the in-memory Chroma transport, the
   deterministic embedder, and the real hive-spi memory registry restored
   around each test. No server, no with-redefs."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-addon.host :as soft]
            [hive-addon.protocol :as addon-proto]
            [hive-chroma.addon :as addon]
            [hive-chroma.client :as client]
            [hive-chroma.connection :as conn]
            [hive-chroma.store :as store]
            [hive-chroma.stub.transport :as transport]
            [hive-chroma.test.fixtures :as fixtures]
            [hive-chroma.test.registry-isolation :as isolation]
            [hive-mcp.embeddings.active :as active]
            [hive-spi.memory.registry :as mem-reg]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defn- chroma-without-server [f]
  (let [prior-transport (client/transport)
        prior-provider  (active/get-embedding-provider)]
    (client/set-transport! (transport/->transport))
    (active/set-embedding-provider! (fixtures/->MockEmbedder 8))
    (conn/reset-collection-cache!)
    (try
      (isolation/with-restored-store-registry
        (fn [] (fixtures/with-config {:embedder {:no-embed-types #{} :routes {}}} (f))))
      (finally
        (conn/reset-collection-cache!)
        (client/set-transport! prior-transport)
        (active/set-embedding-provider! prior-provider)))))

(use-fixtures :each chroma-without-server)

(deftest resolve-config-precedence
  (testing "explicit > env > defaults"
    (is (= {:host "h" :port 1 :collection-name "c" :registry-key :k :active? true :vector-store? false}
           (addon/resolve-config {:host "h" :port 1 :collection-name "c" :registry-key :k
                                  :active? true :vector-store? false}
                                 {"CHROMA_HOST" "env" "CHROMA_PORT" "9"})))
    (is (= {:host "env" :port 9 :collection-name "hive-mcp-memory" :registry-key :chroma
            :active? false :vector-store? true}
           (addon/resolve-config {} {"CHROMA_HOST" "env" "CHROMA_PORT" "9"})))
    (is (= "localhost" (:host (addon/resolve-config nil (constantly nil)))))))

(deftest initialize-registers-and-claims-an-empty-default-slot
  (mem-reg/reset-active-store!)
  (let [a   (addon/create-addon)
        res (addon-proto/initialize! a {:host "chroma.test" :port 9000 :vector-store? false})]
    (is (:success? res))
    (is (true? (get-in res [:metadata :active?])))
    (is (instance? hive_chroma.store.ChromaMemoryStore (:chroma (mem-reg/registered-stores))))
    (is (identical? (:chroma (mem-reg/registered-stores)) (mem-reg/get-store)))
    (is (= :ok (:status (addon-proto/health a))))
    (addon-proto/shutdown! a)
    (is (nil? (:chroma (mem-reg/registered-stores))))
    (is (false? (mem-reg/store-set?)) "shutdown releases the :default slot it held")))

(deftest initialize-never-evicts-another-backend
  (let [other (store/create-store {})]
    (mem-reg/set-store! other)
    (let [a   (addon/create-addon)
          res (addon-proto/initialize! a {:vector-store? false})]
      (is (:success? res))
      (is (false? (get-in res [:metadata :active?])))
      (is (identical? other (mem-reg/get-store)) "the active backend is left in place")
      (addon-proto/shutdown! a)
      (is (identical? other (mem-reg/get-store))))))

(deftest every-declared-host-symbol-resolves
  (is (= {} (soft/unresolvable '[hive-chroma.host]))
      "a host rename must fail here, not degrade silently"))

(deftest addon-identity
  (let [a (addon/create-addon)]
    (is (= "hive.chroma" (addon-proto/addon-id a)))
    (is (contains? (addon-proto/capabilities a) :memory-store))
    (is (= {:status :down :details {:reason "not initialized"}} (addon-proto/health a)))))
