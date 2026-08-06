# hypercube-cache-memory

In-process, in-memory driver for [go-hypercube](https://github.com/go-hypercube/go-hypercube)'s `cache.Cache` interface. No external dependencies — useful for tests, local development, or single-instance deployments.

## Install

```bash
go get go get github.com/go-hypercube/hypercube-cache-memory
```

## Usage

```go
import memorycache "github.com/go-hypercube/hypercube-cache-memory"

cache := memorycache.New()

app := hypercube.New(cfg, db, cache)
```

## Notes

- Implements the full `github.com/go-hypercube/go-hypercube/cachee` interface: `Get`, `Set`, `Delete`, `Has`, `Increment`, `Expire`.
- Misses map to `cache.ErrNotFound`; negative TTLs return `cache.ErrInvalidTTL`.
- Data lives only in the process's memory — nothing is shared across instances or survives a restart. Not suitable for multi-instance deployments; use `hypercube-cache-redis` or `hypercube-cache-memcached` for that.
- A background goroutine sweeps expired keys once a minute; reads also check expiry lazily, so an expired-but-unswept key is never returned as a hit.

## License

MIT
