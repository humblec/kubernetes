# Bump etcd SDK in Kubernetes

Update the pinned etcd client/server modules to a new version (e.g. v3.6.8) and refresh vendor. Follows the same flow as [PR #136407](https://github.com/kubernetes/kubernetes/pull/136407).

## Scope

etcd modules in `go.mod` and staging `go.mod` files:

- `go.etcd.io/etcd/api/v3`
- `go.etcd.io/etcd/client/pkg/v3`
- `go.etcd.io/etcd/client/v3`
- `go.etcd.io/etcd/pkg/v3` (indirect)
- `go.etcd.io/etcd/server/v3` (indirect)
- `go.etcd.io/raft/v3` (indirect; often stays at v3.6.0 unless the etcd release requires a newer raft)

Do **not** edit `go.mod` by hand. Use the repo scripts so staging and vendor stay in sync.

## Workflow

### 1. Pin each etcd module to the target version

From the repository root, run `hack/pin-dependency.sh` for each module with the desired tag (e.g. `v3.6.8`):

```bash
./hack/pin-dependency.sh go.etcd.io/etcd/api/v3 v3.6.8
./hack/pin-dependency.sh go.etcd.io/etcd/client/pkg/v3 v3.6.8
./hack/pin-dependency.sh go.etcd.io/etcd/client/v3 v3.6.8
./hack/pin-dependency.sh go.etcd.io/etcd/pkg/v3 v3.6.8
./hack/pin-dependency.sh go.etcd.io/etcd/server/v3 v3.6.8
```

Only bump `go.etcd.io/raft/v3` if the etcd release notes or go.mod of the new etcd version require it; otherwise leave it as-is (e.g. v3.6.0).

### 2. Rebuild vendor

```bash
./hack/update-vendor.sh
```

Resolve any script or build failures before committing.

### 3. Verify

- `git diff` should show version changes in root and staging `go.mod`/`go.sum` and in `vendor/` and `vendor/modules.txt`.
- Build: `make quick-release` or at least `go build ./cmd/...` (or run the repo's verify scripts).

### 4. Commit and PR

- Branch: e.g. `bump-etcd-sdk-3.6.8`
- Commit message: e.g. `Bump etcd client SDK to 3.6.8`
- PR title: e.g. `Bump etcd client SDK to 3.6.8`
- PR body (template):
  - **What type of PR is this?** `/kind cleanup`
  - **What this PR does / why we need it:** Bumps the etcd client SDK to 3.6.8
  - **Which issue(s) this PR is related to:** (optional, e.g. etcd-io/etcd#xxxxx)
  - **Does this PR introduce a user-facing change?**  
    `Updates the etcd client library to v3.6.8`

## Reference

- [Vendor guide](https://git.k8s.io/community/contributors/devel/sig-architecture/vendor.md): use `hack/pin-dependency.sh` and `hack/update-vendor.sh`; do not edit `go.mod` manually for pinned deps.
- Example PR: [Bump etcd client SDK to 3.6.7](https://github.com/kubernetes/kubernetes/pull/136407).
