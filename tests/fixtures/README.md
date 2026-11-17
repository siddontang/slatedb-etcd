# Test keys

`server.crt` and `server.key.insecure` come from etcd v3.6.5 `tests/fixtures`.
They support the imported RS256 token tests. They are public test data, not deployment credentials.
The source uses Apache License 2.0. See [LICENSE.etcd](../upstream/LICENSE.etcd).
TLS process tests generate temporary certificates and separate trust roots for each cluster.
