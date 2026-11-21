# hive-chroma

The [Chroma](https://www.trychroma.com/) vector database as a hive-mcp backend, mounted
as an **IAddon**, the same way hive-milvus and hive-qdrant are.

It was extracted from hive-mcp's `hive-mcp.chroma.*` and `hive-mcp.memory.store.chroma`,
with their git history kept (`git log --follow src/hive_chroma/crud.clj`).

## What it provides

| Port | Implementation |
|------|----------------|
| `hive-spi.memory.ports/IMemoryStore` (+ Analytics, Staleness, Batch, Scan, Liveness) | `hive-chroma.store/ChromaMemoryStore` |
| the host's `IVectorCollectionStore` (published as `hive-spi.vector.ports`; the host hands its own over `:vector/port`) | `hive-chroma.vector-store` |
| `hive-spi.lifecycle.ports/IShutdownHook` (priority 210) | `hive-chroma.lifecycle` |
| `hive-addon.protocol/IAddon` | `hive-chroma.addon` (manifest `META-INF/hive-addons/hive-chroma.edn`, id `hive.chroma`) |

On `initialize!` the addon registers its store under `:chroma` (`:registry-key`), and takes
the active `:default` slot **only when it is empty** or when `:active? true` is set. It never
evicts another backend. It offers itself as the vector-collection store on the same terms.

## Depends on hive-spi, never on its host

hive-chroma depends on hive-spi (and generic libs) only: hive-mcp is on no classpath, in no
scope, and no host symbol is resolved at runtime either. Every host service (the active
embedding provider, per-type routing and spaces, config, the process-wide embedding gate,
federated ingest search, the host's vector-collection port) arrives through
`hive-spi.vector.host-services`: the host publishes capability -> fn at boot (hive-mcp does
it in `hive-mcp.embeddings.host-services`), and `hive-chroma.host` calls through it. Each
call declares its degraded value for a host that published nothing. The pure
embedding-space layer (stamp, admit, audit) comes straight from `hive-spi.embeddings.space`.

`addon_test` asserts every capability the backend consumes is in hive-spi's catalog.

## Config

`:host` / `:port` / `:collection-name` in the addon config, else `CHROMA_HOST` / `CHROMA_PORT`,
else `localhost:8000` / `hive-mcp-memory`.

## Tests

```
clojure -M:test        # in-memory IChromaTransport stub, no server
clojure -M:test-e2e    # against a live Chroma (CHROMA_HOST/CHROMA_PORT)
```

Tests inject stub adapters at the transport port and a stub host published through hive-spi
(`test/hive_chroma/test/host.clj`). Nothing uses `with-redefs`.

## License

AGPL-3.0-or-later, the license this code carried inside hive-mcp.
