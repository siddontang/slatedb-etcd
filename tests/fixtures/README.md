# Temporary test credentials

Tests generate TLS certificates and JWT keys in temporary directories.
Each JWT cluster uses a new RSA key pair. Test cleanup removes the files.

The historical key and certificate came from the public etcd v3.6.5 test fixtures.
The publication cleanup removed both files from the rewritten project history.
Do not use upstream test credentials in a deployment.
