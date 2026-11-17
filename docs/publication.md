# Publication review

The initial audit covered `main` at `c7280fb` and the heads of pull requests 1 through 6.
Those refs contain 22 commits. The scan also included all other fetched local refs.
Gitleaks 8.30.1 found two private-key entries, both containing the same public etcd test fixture.
Manual checks covered private-key headers, credential file names, credential URLs, local paths,
internal links, and current license notices.
The manual history pass inspected 281 distinct file versions.
No deployment credential was identified in this scope.

## Historical test key

The key came from [etcd v3.6.5](https://github.com/etcd-io/etcd/tree/v3.6.5/tests/fixtures).
Its complete bytes match the pinned reference checkout.
Its SHA-256 is:

```text
3bf06e3687fce3cdd050e32b6f9dfa4eeb0787ee5fd514840b50bae167db3874
```

The historical file is `tests/fixtures/server.key.insecure`.
Only its entries in `c7280fb695b06df1a8cfd434584b42bacf53dbe6` and
`b44fbb52d5aa6f6252db3f55ee4e6d2035eb76c2` have scan exceptions.
The scanner validates their content hashes when those commits are available.
There is no exception for the current path or any future commit.
Current tests generate a new RSA key pair per JWT cluster and remove it during test cleanup.

## Repeat the checks

Install [Gitleaks 8.30.1](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1).
Verify the release archive against its published SHA-256 before running it.
CI pins the Linux archive hash in its workflow.

```sh
python3 scripts/check-public.py
python3 scripts/test_reporting.py
python3 scripts/check-secrets.py
```

Set `GITLEAKS` to the executable path if it is not on PATH.
The scan rejects shallow clones. It checks every fetched ref and snapshots current tracked and
unignored files. Current files have no secret exceptions.
The scan also checks commit and annotated tag messages.
All scanner output redacts credential values. The wrapper prints only finding locations.
Run `python3 scripts/test_secret_scan.py` to check the scanner with a fresh temporary private key.

For an audit that includes closed pull requests, fetch their heads first:

```sh
git fetch --prune origin
git fetch origin '+refs/pull/*/head:refs/audit/pull/*'
python3 scripts/check-secrets.py
```

The script cannot inspect refs that the server does not expose, external forks, account secrets,
release attachments, issue uploads, or private data outside its file snapshot.
Ignored local databases and raw traces are not publication inputs and are not part of this scan.
Do not publish a whole working directory archive. Use a clean clone or `git archive`.

## Metadata and reports

Current reports replace workstation paths with portable labels.
Measurements, outcomes, test counts, seeds, and binary and trace hashes remain unchanged.
The `results/` index distinguishes historical failures from current capability records.

Git history still contains original author names, email addresses, and historical workstation paths.
This change preserves published commits. Deleting or editing current files does not hide that
history.
If those metadata must remain private, review a history rewrite before changing repository
visibility. Existing PR refs and external copies can retain old objects.

## Before changing visibility

- Check that CI and the current secret scan pass on the intended commit.
- Review author metadata and the historical path disclosure described above.
- Keep the Apache license and upstream notices in the public tree.
- Enable GitHub secret scanning, push protection, and private vulnerability reporting where
  available.
- Review repository collaborators, branch protection, Actions permissions, and stored Actions
  secrets.
- Publish selected reports only after checking their contents for user data and credentials.

Changing repository visibility is a separate owner action.
This review is evidence for the listed scope, not a guarantee that all sensitive data is absent.
