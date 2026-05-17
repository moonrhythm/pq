# CLAUDE.md

## Fork context

This repo (`moonrhythm/pq`) is a **fork** of `lib/pq`. The Go module path is still `github.com/lib/pq` (do not rename it — changing it would break every importer).

## Always target moonrhythm/pq for PRs and issues

`gh` infers the PR/issue target from the GitHub fork relationship, and for forks it defaults to the **upstream** (`lib/pq`). That is almost never what we want here.

When creating PRs or issues from this repo, always pass `--repo moonrhythm/pq` explicitly:

```sh
gh pr create --repo moonrhythm/pq --base master --head <branch> --title ... --body ...
gh issue create --repo moonrhythm/pq --title ... --body ...
```

The same applies to any `gh pr`/`gh issue` subcommand (`view`, `list`, `edit`, `close`, etc.) when the default would resolve to `lib/pq`.

## Merging from upstream

The `lib/pq` upstream is configured as the `upstream` remote. To pull in new commits:

```sh
git fetch upstream
git merge upstream/master
```
