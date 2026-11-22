(ns hive-chroma.vector-store
  "Chroma as the host's `IVectorCollectionStore`.

   This is the ADAPTER, and the one place allowed to know both sides: the
   host's named-collection port and the vendor transport in
   `hive-chroma.client`.

   The port the host consumes is handed over at runtime (the `:vector/port`
   host service; with no host, the published `hive-spi.vector.ports` one), so
   the record is extended onto it at FACTORY time with `clojure.core/extend`,
   keyed by the protocol's own method keys (`hive-chroma.host/vector-port`).
   A standalone compile needs no host. This is the shape
   hive-milvus.vector-store uses.

   The vendor client is already record-oriented: `add` takes embedding records,
   `get` and `query` return them, and `query` is documented as ordered by
   increasing distance. So this adapter does NOT reshape payloads. It absorbs
   the two things every call site was otherwise repeating by hand:

   - the client returns FUTURES, so each caller wrapped itself in
     `ws/deref-safe!` with its own timeout. Reads were 15s and writes 30s at
     the call sites; those are kept, so nothing's timeout changes.
   - the client's option names are its own (`:num-results`, `:get-or-create`).
     The port speaks `:n-results` and `:get-or-create?`, and translating
     between the two is exactly why an adapter exists rather than an alias."
  (:require [clojure.string :as str]
            [hive-chroma.client :as client]
            [hive-chroma.host :as host]
            [hive-weave.safe :as ws]
            [malli.core :as m]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:private read-timeout-ms 15000)
(def ^:private write-timeout-ms 30000)

(def ^:private default-include
  "What a read asks the vendor to return. The port always promises the record
   fields, so a caller never has to remember this set."
  #{:documents :metadatas :distances})

(defn- await!
  "Resolve a vendor result to a value.

   Three shapes, because three things produce them: the real client returns a
   `Future`, which must be bounded (`deref-safe!` casts to Future and is the
   only form that can time out); a test transport may return another IDeref;
   and a stub may return the value itself."
  [v timeout-ms]
  (cond
    (instance? java.util.concurrent.Future v) (ws/deref-safe! v timeout-ms)
    (instance? clojure.lang.IDeref v)         @v
    :else                                     v))

(defrecord ChromaVectorStore [transport])

;;; Port method implementations, each a plain fn of [this & args].

(defn- -configure* [this opts]
  (client/-configure (:transport this) opts)
  this)

(defn- -get-collection* [this coll-name]
  (await! (client/-get-collection (:transport this) coll-name) read-timeout-ms))

(defn- -create-collection* [this coll-name opts]
  (await! (client/-create-collection
           (:transport this) coll-name
           (cond-> {}
             (:metadata opts)       (assoc :metadata (:metadata opts))
             (:get-or-create? opts) (assoc :get-or-create true)))
          write-timeout-ms))

(defn- -delete-collection* [this coll]
  (await! (client/-delete-collection (:transport this) coll) write-timeout-ms)
  nil)

(defn- -add* [this coll records opts]
  (await! (client/-add (:transport this) coll (vec records)
                       (select-keys (or opts {}) [:upsert?]))
          write-timeout-ms)
  nil)

(defn- -get* [this coll {:keys [ids where limit include]}]
  (vec (await! (client/-get (:transport this) coll
                            (cond-> {:include (or include default-include)}
                              (seq ids) (assoc :ids (vec ids))
                              where     (assoc :where where)
                              limit     (assoc :limit limit)))
               read-timeout-ms)))

(defn- -query* [this coll embedding {:keys [n-results where include]}]
  ;; The port promises NEAREST FIRST on an ascending :distance, and the
  ;; vendor documents that order. Sorting anyway is the cheap guard: a
  ;; backend that ever passed a cosine SIMILARITY through :distance would
  ;; otherwise reverse every caller.
  (->> (await! (client/-query (:transport this) coll embedding
                              (cond-> {:include (or include default-include)}
                                n-results (assoc :num-results n-results)
                                where     (assoc :where where)))
               read-timeout-ms)
       (sort-by #(or (:distance %) 0))
       vec))

(defn- -delete* [this coll {:keys [ids where]}]
  (await! (client/-delete (:transport this) coll
                          (cond-> {}
                            (seq ids) (assoc :ids (vec ids))
                            where     (assoc :where where)))
          write-timeout-ms)
  nil)

(defn- -update* [this coll records]
  (await! (client/-update (:transport this) coll (vec records)) write-timeout-ms)
  nil)

(def ^:private method-impls
  "Port method name (no leading dash) -> its implementation."
  {"configure"         -configure*
   "get-collection"    -get-collection*
   "create-collection" -create-collection*
   "delete-collection" -delete-collection*
   "add"               -add*
   "get"               -get*
   "query"             -query*
   "delete"            -delete*
   "update"            -update*})

(defn- impl-map
  "Pure: the `extend` map for a protocol whose method keys are `sig-keys`.
   Each key, `:-get-collection` or `:get-collection`, maps to the impl of that
   method name; a key with no impl is absent."
  [sig-keys]
  (into {}
        (keep (fn [k]
                (let [n (name k)
                      f (get method-impls (if (str/starts-with? n "-") (subs n 1) n))]
                  (when f [k f]))))
        sig-keys))

(m/=> impl-map [:=> [:cat [:sequential :keyword]] [:map-of :keyword ifn?]])

(defn- install!
  "Extend the record onto PROTO, keyed by the protocol's OWN method keys so a
   renamed port method cannot silently go unimplemented. Throws when a port
   method has no implementation here. Returns PROTO."
  [proto]
  (let [sig-keys (keys (:sigs proto))
        impls    (impl-map sig-keys)
        missing  (remove (set (keys impls)) sig-keys)]
    (when (seq missing)
      (throw (ex-info (str "hive-chroma implements no " (vec missing)
                           " of the host's IVectorCollectionStore")
                      {:missing (vec missing)})))
    (extend ChromaVectorStore proto impls)
    proto))

(m/=> install! [:=> [:cat [:map [:sigs :map]]] :map])

(defn chroma-vector-store
  "A store over the active Chroma transport, or over TRANSPORT when given.
   With PROTO the record is extended onto that protocol (tests pass the
   published hive-spi port); without it, onto the host's port. A runtime with
   no port at all throws with a reason."
  ([] (chroma-vector-store (client/transport)))
  ([transport] (chroma-vector-store transport (host/vector-port)))
  ([transport proto]
   (when-not proto
     (throw (ex-info "No IVectorCollectionStore port on the classpath"
                     {:error :hive-chroma/no-vector-port})))
   (install! proto)
   (->ChromaVectorStore transport)))

(def VectorStore
  "A ChromaVectorStore. Generates one over the soft vendor transport."
  [:fn {:gen/schema client/Transport :gen/fmap ->ChromaVectorStore}
   #(instance? ChromaVectorStore %)])

(def ^:private Records [:sequential :map])
(def ^:private ReadOpts
  [:maybe [:map [:ids {:optional true} [:maybe [:sequential :any]]]
           [:where {:optional true} [:maybe :map]]
           [:limit {:optional true} [:maybe pos-int?]]
           [:n-results {:optional true} [:maybe pos-int?]]
           [:include {:optional true} :any]]])

(m/=> await! [:=> [:cat :any pos-int?] :any])
(m/=> -configure* [:=> [:cat VectorStore :map] VectorStore])
(m/=> -get-collection* [:=> [:cat VectorStore :string] client/Coll])
(m/=> -create-collection* [:=> [:cat VectorStore :string [:maybe :map]] client/Coll])
(m/=> -delete-collection* [:=> [:cat VectorStore client/Coll] :nil])
(m/=> -add* [:=> [:cat VectorStore client/Coll Records [:maybe :map]] :nil])
(m/=> -get* [:=> [:cat VectorStore client/Coll ReadOpts] [:vector :map]])
(m/=> -query* [:=> [:cat VectorStore client/Coll [:sequential number?] ReadOpts] [:vector :map]])
(m/=> -delete* [:=> [:cat VectorStore client/Coll ReadOpts] :nil])
(m/=> -update* [:=> [:cat VectorStore client/Coll Records] :nil])

(m/=> chroma-vector-store [:function
                           [:=> [:cat] VectorStore]
                           [:=> [:cat client/Transport] VectorStore]
                           [:=> [:cat client/Transport :any] VectorStore]])
