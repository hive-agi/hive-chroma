(ns hive-chroma.addon
  "IAddon for the Chroma vector database backend.

   On initialize! it configures the Chroma connection, builds a
   ChromaMemoryStore, connects it and registers it in the hive-spi memory
   registry: under `:registry-key` (default `:chroma`), and as the ACTIVE
   store only when no other backend holds that slot (an addon never evicts
   another backend; `:active? true` in the config forces it, which is what a
   deployment whose `:services :memory-store :backend` is chroma wants).

   It also offers Chroma as the host's IVectorCollectionStore, again only
   when none is installed yet (hive-chroma.host/install-vector-store!).

   Config (manifest :addon/config, overridden by the init config):
     :host :port :collection-name   the Chroma endpoint (defaults
                                    CHROMA_HOST / CHROMA_PORT env, then
                                    localhost:8000, hive-mcp-memory)
     :registry-key                  registry slot, default :chroma
     :active?                       claim the :default slot even when taken
     :vector-store?                 offer the vector-collection port, default true

   No namespace here requires the host: everything host-side goes through
   hive-chroma.host, i.e. through hive-spi's host-services registry, so this
   loads, and degrades, with no host at all."
  (:require [hive-addon.protocol :as addon-proto]
            [hive-chroma.client :as client]
            [hive-chroma.connection :as conn]
            [hive-chroma.host :as host]
            [hive-chroma.lifecycle :as lifecycle]
            [hive-chroma.store :as store]
            [hive-chroma.vector-store :as vs]
            [hive-spi.memory.ports :as mem-ports]
            [hive-spi.memory.registry :as mem-reg]
            [malli.core :as m]
            [taoensso.timbre :as log]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def addon-id "hive.chroma")

(def Config
  "What initialize! reads; every key optional."
  [:map
   [:host {:optional true} :string]
   [:port {:optional true} [:or :int :string]]
   [:collection-name {:optional true} :string]
   [:registry-key {:optional true} :keyword]
   [:active? {:optional true} :boolean]
   [:vector-store? {:optional true} :boolean]])

(defn- env [k] (System/getenv k))

(defn resolve-config
  "Pure over ENV-FN: the effective connection + registration config.
   Explicit config wins, then CHROMA_HOST / CHROMA_PORT, then defaults."
  ([cfg] (resolve-config cfg env))
  ([cfg env-fn]
   (let [port (or (:port cfg) (env-fn "CHROMA_PORT") 8000)]
     {:host            (or (:host cfg) (env-fn "CHROMA_HOST") "localhost")
      :port            (if (string? port) (parse-long port) port)
      :collection-name (or (:collection-name cfg) "hive-mcp-memory")
      :registry-key    (or (:registry-key cfg) :chroma)
      :active?         (boolean (:active? cfg))
      :vector-store?   (if (contains? cfg :vector-store?) (boolean (:vector-store? cfg)) true)})))

(m/=> resolve-config [:function
                      [:=> [:cat [:maybe Config]] :map]
                      [:=> [:cat [:maybe Config] ifn?] :map]])

(defn- claim-default?
  "Whether this store should take the :default (active) slot."
  [{:keys [active?]}]
  (or active? (not (mem-reg/store-set?))))

(defn- offer-vector-store!
  "Offer Chroma as the host's vector-collection store; never fatal."
  []
  (try
    (let [outcome (host/install-vector-store! (vs/chroma-vector-store (client/transport)))]
      (log/info "hive-chroma vector-collection store:" outcome)
      outcome)
    (catch Throwable t
      (log/warn "hive-chroma vector-collection store not installed (non-fatal):" (ex-message t))
      :failed)))

(defrecord ChromaAddon [state]
  addon-proto/IAddon

  (addon-id [_] addon-id)

  (addon-type [_] :native)

  (capabilities [_] #{:memory-store :vector-search :health-reporting})

  (initialize! [_ addon-config]
    (try
      (let [cfg   (resolve-config (or addon-config {}))
            conn* (select-keys cfg [:host :port :collection-name])
            _     (conn/configure! conn*)
            st    (store/create-store conn*)
            res   (mem-ports/connect! st conn*)]
        (if-not (:success? res)
          {:success? false :errors (vec (:errors res)) :metadata {:backend "chroma"}}
          (let [active? (claim-default? cfg)]
            (mem-reg/register-store! (:registry-key cfg) st)
            (when active? (mem-reg/set-store! st))
            (lifecycle/install!)
            (let [vec-outcome (when (:vector-store? cfg) (offer-vector-store!))]
              (reset! state {:store st :config cfg :active? active?})
              (log/info "ChromaAddon initialized" {:host (:host cfg) :port (:port cfg)
                                                   :registry-key (:registry-key cfg)
                                                   :active? active?})
              {:success? true :errors []
               :metadata {:backend "chroma" :active? active?
                          :registry-key (:registry-key cfg)
                          :vector-store vec-outcome}}))))
      (catch Throwable t
        {:success? false :errors [(ex-message t)] :metadata {:backend "chroma"}})))

  (shutdown! [_]
    (when-let [{st :store cfg :config} @state]
      (when (identical? st (:default (mem-reg/registered-stores)))
        (mem-reg/reset-active-store!))
      (mem-reg/unregister-store! (:registry-key cfg))
      (mem-ports/disconnect! st)
      (reset! state nil))
    nil)

  (tools [_] [])

  (schema-extensions [_] {})

  (health [_]
    (if-let [st (:store @state)]
      (let [h (mem-ports/health-check st)]
        {:status (if (:healthy? h) :ok :down) :details h})
      {:status :down :details {:reason "not initialized"}}))

  (excluded-tools [_] #{}))

(defn create-addon
  "A ChromaAddon. The optional config is applied at initialize!."
  ([] (->ChromaAddon (atom nil)))
  ([_config] (->ChromaAddon (atom nil))))

(m/=> create-addon [:function
                    [:=> [:cat] [:fn #(instance? ChromaAddon %)]]
                    [:=> [:cat :any] [:fn #(instance? ChromaAddon %)]]])
