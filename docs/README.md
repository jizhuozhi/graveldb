# GravelDB Design Documents

Internal design documentation organized by topic. Each document focuses on one core subsystem, explaining the problem it solves, alternatives considered, and implementation details.

## Documents

| Document | Topic |
|----------|-------|
| [01-overview.md](01-overview.md) | Goals, non-goals, and overall architecture |
| [02-storage-layout.md](02-storage-layout.md) | Per-dim slab files, allocator, DimRegistry |
| [03-buffer-cache.md](03-buffer-cache.md) | WriteBuffer, ReadCache, TinyLFU admission |
| [04-checkpoint.md](04-checkpoint.md) | Overlay isolation, DirtyTracker, delta files, recovery |
| [05-io.md](05-io.md) | I/O strategy, io_uring batch flush, peephole merge |
| [06-server.md](06-server.md) | Parameter server, wire protocol, event loop |

## Relationship to README

The project [README](../README.md) provides a high-level "Design Decisions" section structured as problem→alternatives→choice. These docs go deeper into implementation specifics for contributors and future-self reference.
