# Issue, branch, pull request, and merge lifecycle

## Current transition for issues 1 and 2

Issue 1 was completed in local commit `7de54a5` on `main`. Issue 2 is already
underway. For this one-time transition, finish issue 2 and then place the issue 1
and issue 2 commits on a feature branch:

```bash
git switch -c feat/control-plane-foundations
```

Before committing, inspect the working tree and ensure local preferences, secrets,
temporary files, and unrelated changes are excluded:

```bash
git status --short
git diff
```

Commit issue 2 separately from the existing issue 1 commit, then push the branch:

```bash
git push -u origin feat/control-plane-foundations
```

Create a pull request targeting the repository's default `main` branch. Its body
should include:

```text
Closes #1
Closes #2
```

GitHub will link the pull request to both issues and automatically close them only
after the pull request is merged into the default branch. A branch name or ordinary
push does not close or authoritatively link an issue.

After CI and review pass:

1. Merge the pull request.
2. Confirm issues 1 and 2 closed automatically.
3. Refresh local `main`.
4. Delete the merged feature branch.

```bash
git switch main
git pull
git branch -d feat/control-plane-foundations
```

The remote branch may also be deleted through GitHub or with:

```bash
git push origin --delete feat/control-plane-foundations
```

## Standard lifecycle for issue 3 onward

Use one branch and one pull request per issue unless there is a deliberate reason
to combine tightly coupled issues.

### 1. Refresh the default branch

```bash
git switch main
git pull --ff-only
```

The working tree should be clean before starting.

### 2. Create an issue branch

Use a descriptive name that includes the issue number:

```bash
git switch -c issue-3-stagegraph-orchestration
```

The branch name is helpful to humans, but it does not by itself link the branch to
the GitHub issue.

### 3. Implement and verify

- Follow the local ticket and governing `biotech-meta/docs` specification.
- Commit coherent changes with messages describing why they are needed.
- Run targeted checks while developing and the full required verification before
  handoff.
- Do not include `.env`, credentials, local Cursor preferences, generated caches,
  or unrelated work.

### 4. Push the branch

```bash
git push -u origin issue-3-stagegraph-orchestration
```

### 5. Open a pull request

Target `main` and include:

- a concise summary;
- the verification or test plan;
- known deferrals or follow-up work;
- `Closes #3` on its own line.

The `Closes #N`, `Fixes #N`, or `Resolves #N` keyword in a pull request creates
the closure relationship. The issue closes when that pull request merges into the
default branch.

### 6. Review and update the same branch

Address CI failures and review findings with additional commits on the feature
branch. Push those commits normally; the pull request updates automatically.
Avoid rewriting shared history unless explicitly coordinated.

### 7. Merge

Merge only after:

- required checks pass;
- review findings are resolved;
- the pull request still matches the issue scope;
- documentation and handoff notes are current.

Prefer the repository's established merge strategy. If no strategy has been
established, use a normal GitHub merge or squash merge consistently rather than
choosing differently for every issue.

### 8. Confirm closure and clean up

After merge:

```bash
git switch main
git pull --ff-only
git branch -d issue-3-stagegraph-orchestration
```

Confirm the linked issue closed and that any explicitly deferred work has its own
issue rather than being left only in review comments.

## Lifecycle summary

```text
Issue
  -> branch from updated main
  -> implement and test
  -> commit
  -> push branch
  -> pull request with "Closes #N"
  -> CI and review
  -> merge into main
  -> automatic issue closure
  -> update local main and delete branch
```
