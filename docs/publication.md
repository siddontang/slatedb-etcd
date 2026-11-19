# Publication review

The history cleanup covered 25 commits, including the seven available pull request heads.
It preserved commit order, parent links, author names, and dates.
Author and committer fields now use GitHub public email addresses.
Every rewritten commit has this owner-approved trailer:

```text
Signed-off-by: siddontang <siddontang@gmail.com>
```

This email remains public by request. A sign-off is not a cryptographic signature.
The rewrite invalidates old commit IDs and removes old cryptographic signatures.

## File audit

The rewrite removed the former etcd test private key and certificate from every fetched history.
The key was public test material, not a deployment credential. No secret scan exceptions remain.
Current tests generate temporary TLS and JWT credentials.

The comparison checked every file in all 25 rewritten commits.
It changed 44 historical report versions and 276 distinct path strings.
It preserved all other report values, outcomes, counts, seeds, measurements, and hashes.
It preserved business and test code during the history rewrite.
New repository checks and report normalization are separate changes at the branch tip.

Gitleaks 8.30.1 checks current publication files, all fetched history, and commit and tag messages.
The metadata check also inspects author fields, sign-offs, deleted blobs, and annotated tags.
It accepts the owner sign-off, or one author sign-off with the author's GitHub public email.
Negative controls reject new private keys, missing sign-offs, private authors, and deleted paths.
No deployment credential was identified in the audited source, reports, or downloaded Actions logs.

## Repeat the checks

Install [Gitleaks 8.30.1](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1).
Verify the release archive against its published SHA-256 before running it.
CI pins the Linux archive hash in its workflow.

```sh
python3 scripts/check-public.py
python3 scripts/test_reporting.py
python3 scripts/test_history.py
python3 scripts/check-history.py
python3 scripts/test_secret_scan.py
python3 scripts/check-secrets.py
```

Set `GITLEAKS` to the executable path if it is not on PATH.
The history checks reject shallow clones. They check every fetched ref.
The secret check also snapshots current tracked and unignored files.
Scanner output redacts credential values and reports locations only.

Use a separate audit clone to inspect server-retained pull request heads:

```sh
git fetch origin '+refs/pull/*/head:refs/audit-heads/*'
python3 scripts/check-history.py
python3 scripts/check-secrets.py
```

A normal clone cannot prove that server-retained PR refs or cached commits are clean.
Do not merge an old clone or PR branch into rewritten history. Re-clone or rebase reviewed changes.

## Repository separation

The old repository was renamed to `slatedb-etcd-private-archive` and remains private.
It retains the old PR refs, cached commits, and Actions records for private reference.
Do not change the archive's visibility.

A separate private repository now uses the `slatedb-etcd` name.
Only the cleaned `main` history was pushed to that repository.
No old PR refs, tags, audit refs, logs, or release assets were imported.
Historical commit subjects retain PR numbers from the private archive.
Those numbers do not identify PRs in the new repository.

Verification uses a fresh clone from the new repository and enumerates all advertised refs.
It checks sign-offs, commit metadata, historical blobs, current files, and secret patterns.
It also checks that representative old commits cannot be fetched from the new repository.
Full CI runs against the imported branch in the new repository.

GitHub retains old PR refs and cached commit views after a branch rewrite.
Repository separation keeps those records outside the repository intended for publication.
See GitHub's [sensitive data removal guide][removal] for the limits of a branch rewrite.

[removal]: https://docs.github.com/articles/removing-sensitive-data-from-a-repository

A history rewrite cannot remove copies held by other users.
The private archive and local recovery copies still contain the original metadata.

## Scope and remaining owner actions

Ignored local databases, raw traces, and private recovery backups are not publication inputs.
Use a clean clone or `git archive`; do not upload the whole working directory.
The private recovery archive contains the original history and must stay private.

Before changing visibility:

- Check the intended branch, tags, fetched PR refs, and current CI result.
- Keep the old archive private and confirm that the new repository has only clean refs.
- Accept the deliberate public owner email in every sign-off.
- Keep the Apache license and upstream notices.
- Enable available secret scanning, push protection, and private vulnerability reporting.
- Review collaborators, branch protection, Actions permissions, and stored Actions secrets.

Changing repository visibility is a separate owner action.
This review covers the listed inputs. It cannot prove that all sensitive data is absent everywhere.
