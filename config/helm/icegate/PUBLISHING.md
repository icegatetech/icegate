# Publishing this chart to Artifact Hub

Everything in this file is a manual, account-level action. The repository content —
annotations, values schema, README, ownership claim, signing — is maintained by the
chart itself and validated by `make helm-metadata-test`.

## Order matters

Artifact Hub issues the `repositoryID` when the repository is first registered, and
`artifacthub-repo.yml` needs that ID. So registration comes first, the ID second, and
it reaches the registry on the release after that.

## 1. Confirm the chart is anonymously pullable

Artifact Hub reads the registry without credentials. Verify:

```bash
TOKEN=$(curl -s "https://ghcr.io/token?scope=repository:icegatetech/charts/icegate:pull&service=ghcr.io" \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
curl -s -H "Authorization: Bearer $TOKEN" \
  https://ghcr.io/v2/icegatetech/charts/icegate/tags/list
```

Expected: a JSON tag list. If this returns an error, the GHCR package is private —
open the package on `github.com/orgs/icegatetech/packages`, then Package settings,
and change visibility to public.

## 2. Register the repository

In the Artifact Hub UI, under Control Panel then Repositories, add a repository:

- Kind: Helm charts
- URL: `oci://ghcr.io/icegatetech/charts/icegate`

The OCI URL points at the chart, not at the namespace above it: one Artifact Hub
repository entry corresponds to one chart.

## 3. Record the repository ID

Copy the ID Artifact Hub shows for the new repository into
`config/helm/icegate/artifacthub-repo.yml`:

```yaml
repositoryID: <the-id-from-the-ui>
owners:
  - name: icegatetech
    email: contact@icegate.team
```

Commit it. The next tagged release pushes it to the registry, and Artifact Hub picks
it up on its next scan.

## 4. Verify the listing

After the next release and Artifact Hub's scan — which runs on their schedule, not
ours, so allow for a lag — check the package page for:

- The values browser, from `values.schema.json`
- A security report, from `artifacthub.io/images`
- The Signed badge, from the cosign signature
- The Verified Publisher badge, from `artifacthub-repo.yml`
- A changelog, from `artifacthub.io/changes`

A missing security report almost always means the image tags in
`artifacthub.io/images` name a version that was never published. `make
helm-metadata-test` catches the mismatch against `appVersion`, but it cannot know
whether the release actually succeeded — check the release workflow run.
