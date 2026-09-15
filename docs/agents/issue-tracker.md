# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- Create issues with `gh issue create`.
- Read issues with `gh issue view <number> --comments`.
- List and filter issues with `gh issue list`.
- Comment with `gh issue comment <number>`.
- Apply or remove labels with `gh issue edit <number> --add-label/--remove-label`.
- Close with `gh issue close <number> --comment`.

Infer the repository from `git remote -v`; the current remote is `https://github.com/GhostAdults/Relus.git`.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## When a skill says “publish to the issue tracker”

Create a GitHub issue.

## When a skill says “fetch the relevant ticket”

Run `gh issue view <number> --comments`.
