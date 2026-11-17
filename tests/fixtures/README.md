# Temporary test keys

TLS tests generate fresh certificates and separate trust roots for every cluster.
JWT tests generate a fresh RSA key pair in a test temporary directory.
Private key files have mode 0600. Test cleanup removes the files.
No committed certificate or private key is required.

Earlier commits used the public etcd v3.6.5 `server.crt` and `server.key.insecure` fixtures.
See the [publication review](../../docs/publication.md) for their provenance and historical scan
exceptions. Never use a test key as a deployment credential.
