# Contributing to AchillesDB

## Requirements

- You must have WiredTiger and FAISS installed (built from source or via package manager) and available for CGO.

## Branches

- `main`: stable production
- `dev`: integration & features
- `feature/*`: new features (from `dev`)
- `release/*`: release prep (from `dev`, merge to `main`)

## Workflow

**Start a feature:**

```bash
git checkout dev
git pull
git checkout -b feature/my-feature
# work and commit
git push -u origin feature/my-feature
# Open PR to dev
```

**Release:**

```bash
git checkout dev
git pull
git checkout -b release/vX.Y.Z
# prep & commit
# Open PR: release/vX.Y.Z → main
# After merging:
git checkout dev
git merge release/vX.Y.Z
git push
```

## Style

- Run `go fmt` and `go mod tidy` before commit.

## Testing

```bash
go test ./...      # (requires WiredTiger and FAISS installed)
CGO_ENABLED=0 go test -tags nocgo ./...   # (limited)
```
