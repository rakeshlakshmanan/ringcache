package client

import "github.com/rakeshlakshmanan/ringcache/internal/ring"

// WithInitialRing starts a client from a given ring instead of fetching
// one from the seeds. Only a test wants this: it is how a client is put
// deliberately behind the cluster, which is the state FR-11's refresh
// has to recover from.
//
// It is exported to tests in other packages through this file, so the
// option does not appear in the package's public API.
func WithInitialRing(v ring.View) Option { return withInitialRing(v) }
