// Package metrics owns the Prometheus collectors listed in docs/10-operations.md §5.
//
// Collectors are constructed once and passed explicitly to whatever records them. They
// are not package-level singletons and must not become any: promauto's default registerer
// is a global, and a global registry makes two tests in one process collide and hides
// which component owns which metric.
//
// What this package must never do:
//
//   - Register against prometheus.DefaultRegisterer. The registry is constructed by the
//     caller and handed in.
//   - Use a channel name as a label. Channel names become metric labels only in the admin
//     API; a namespace with one channel per user and 200,000 users will hurt Prometheus.
//     Namespaces are labelled, individual channels are not — that is the reason for the
//     split (docs/06-channels.md §2).
//   - Put a cookie, a user id, an origin, or any payload-derived value in a label
//     (NFR-7).
//   - Grow a metric that does not appear in docs/10-operations.md §5 without adding it
//     there in the same commit. An operator's alerts are written against that table.
package metrics
