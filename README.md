# ringcache

ringcache is a self-healing distributed in-memory cache written in Go. It runs as 3 to 7 node processes that clients use as one cache: keys are spread over the nodes with consistent hashing and every key is stored on two of them. Membership lives in etcd, so every node agrees on one ring and each change is an atomic transaction. When a node dies the cluster notices within about 2 seconds, serves traffic from the surviving copy and restores a second copy on its own; nodes join and leave under load without client errors.

## Build

```sh
make build   # compiles every binary under cmd/ into bin/
make check   # go vet, staticcheck and go test -race
```
