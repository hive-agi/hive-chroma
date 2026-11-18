# hive-chroma

The [Chroma](https://www.trychroma.com/) vector database as a hive-mcp backend, mounted
as an **IAddon**, the same way hive-milvus and hive-qdrant are.

It was extracted from hive-mcp's `hive-mcp.chroma.*` and `hive-mcp.memory.store.chroma`,
with their git history kept (`git log --follow src/hive_chroma/crud.clj`).

## What it provides

| Port | Implementation |
|------|----------------|
| `hive-spi.memory.ports/IMemoryStore` (+ Analytics, Staleness, Batch, Scan, Liveness) | `hive-chroma.store/ChromaMemoryStore` |
| the host's `IVectorCollectionStore` (`hive-mcp.protocols.vector`, published as `hive-spi.vector.ports`) | `hive-chroma.vector-store` |
| `hive-spi.lifecycle.ports/IShutdownHook` (priority 210) | `hive-chroma.lifecycle` |
| `hive-addon.protocol/IAddon` | `hive-chroma.addon` (manifest `META-INF/hive-addons/hive-chroma.edn`, id `hive.chroma`) |

On `initialize!` the addon registers its store under `:chroma` (`:registry-key`), and takes
the active `:default` slot **only when it is empty** or when `:active? true` is set. It never
evicts another backend. It offers itself as the vector-collection store on the same terms.

## No compile-time host dependency

No namespace under `src/` requires hive-mcp. Every host service (the active embedding
provider, per-type routing, embedding spaces, config, the process-wide embedding gate,
federated ingest search) is soft-resolved through `hive-chroma.host` (`hive-addon.host/defsoft`).
Each one declares its degraded value, and `addon_test` asserts that every declared host
symbol resolves against the pinned hive-mcp.

## Config

`:host` / `:port` / `:collection-name` in the addon config, else `CHROMA_HOST` / `CHROMA_PORT`,
else `localhost:8000` / `hive-mcp-memory`.

## Tests

```
clojure -M:test        # in-memory IChromaTransport stub, no server
clojure -M:test-e2e    # against a live Chroma (CHROMA_HOST/CHROMA_PORT)
```

Tests inject stub adapters at the transport and embedder ports. Nothing uses `with-redefs`.

## License

AGPL-3.0-or-later, the license this code carried inside hive-mcp.
