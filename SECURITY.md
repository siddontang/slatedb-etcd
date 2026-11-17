# Security policy

This project is experimental software. It has no supported production release or security
maintenance schedule. Do not use it as a Kubernetes backend or assume complete etcd compatibility.

## Report an issue

Use GitHub's private vulnerability reporting when it is enabled for this repository:

[Report a vulnerability](https://github.com/siddontang/slatedb-etcd/security/advisories/new)

If that option is unavailable, open an issue that only requests a private contact channel.
Do not include credentials, private keys, customer data, or exploit details in a public issue.
Provide a minimal reproduction, the affected commit, and the observed impact through the private
channel.

## Trust boundaries

Listeners bind to loopback by default. Remote mode requires mutual TLS on separate client and peer
ports. Local users and processes can still connect to loopback ports.
The default shared plaintext listener exposes trusted diagnostic and fault controls.
Those controls, including Raft transport, are outside etcd Auth enforcement.
Enabling Auth alone does not make that shared listener safe for untrusted local users.

Use separate client and peer listeners with mutual peer TLS for identity separation.
A peer certificate grants access to trusted forwarding and diagnostic controls.
Keep peer and client CAs separate, and restrict peer private keys to trusted operators.
Certificate reload and automatic certificate generation remain unsupported.
Remote operation has controlled three-guest coverage, not independent physical host validation.
Do not expose the service through a public proxy or port forward.

Passwords use bcrypt. Auth changes invalidate tokens and replicate through Raft.
Token expiry uses server time. Clock faults and independent host failures remain outside the
validated scope.
Data directories and backups contain sensitive values and authentication state.
Protect them with operating system permissions and storage controls.

## Repository hygiene

Tests generate fresh TLS and JWT keys in temporary directories.
No private key is needed in the current repository files.
The rewritten history removes the former public etcd test key. No scan exceptions remain.
The [publication review](docs/publication.md) records the audit and remote retention limits.
Never use a test key or test password as a deployment credential.

Run the secret scan before publishing commits or attaching reports.
A passing pattern scan cannot prove that all sensitive data is absent.
If a real credential enters Git history, revoke or rotate it before removing the file.
Deleting a current file alone does not remove older commits or copied credentials.
