# Contributing to AchillesDB

## Branches

- `main`: production-ready
- `dev`: feature integration
- `feature/*`: new features (branch off `develop`)
- `release/*`: prep for releases (from `develop`, merge to `main`)

## Typical Workflow

**Start a feature:**

```bash
git checkout develop
git pull
git checkout -b feature/my-feature
# work & commit
git push -u origin feature/my-feature
# Open PR to dev
```

**Create a release:**

```bash
git checkout dev
git pull
git checkout -b release/vX.Y.Z
# finalize & commit
# PR: release/vX.Y.Z → main
# After merge/tag:
git checkout develop
git merge release/vX.Y.Z
git push
```

**Hotfixes:**  
Branch from `main`, PR to `main`, then merge fix into `dev`.

## CI/CD

- `ci.yml`: tests, lint, Docker build on PRs/main/develop
- `release.yml`: creates GitHub release on tag
- `docker-publish.yml`: pushes Docker image on tag

## Style

- Run `go fmt` and `go mod tidy` before committing

## Testing

```bash
go test ./...                     # (needs WiredTiger/FAISS)
CGO_ENABLED=0 go test -tags nocgo ./...   # (limited)
```
