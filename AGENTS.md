# AI 开发入口

修改、调试或审查本仓库前，必须完整阅读并遵循根目录的 [`CONTRIBUTING.md`](CONTRIBUTING.md)。其中定义了项目架构、数据契约、数据源插件化、缓存与性能要求、测试矩阵以及 PR 复审和合并标准。

同时遵守以下规则：

- 先理解调用链和现有测试，再进行修改。
- 保持实现简单、改动范围最小，不处理无关问题。
- 不覆盖工作区已有修改，不虚构测试或审查结果。
- 以实际验证结果作为完成标准。

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. Commit Every Code Change

**Make completed code changes traceable in Git.**

- After a code change is verified, create a focused Git commit before finishing the task.
- Commit only files related to that change. Keep unrelated or pre-existing user changes out of the commit.
- Use a descriptive commit message that states the behavior or problem changed.
- Review the staged diff before committing, and do not commit generated artifacts or secrets.
- Report the commit hash and a short change summary in the final response.
- Do not push commits or open a pull request unless the user explicitly asks.
- If a commit cannot be created safely, explain the blocker instead of silently leaving verified code uncommitted.

## 6. Parallel Development Workflow

**When parallel development is detected in the current repository, isolate the task by default. Do not wait for the user to call it out.**

- Before editing, inspect the current worktree, existing Git worktrees, and available task or agent state for parallel activity.
- Treat another active task, agent, or worktree, and unrelated changes that clearly belong to other work, as evidence of parallel development.
- Create a dedicated feature branch in a separate Git worktree from the target branch's latest committed HEAD.
- Never carry, stash, revert, stage, or commit unrelated changes from the target worktree.
- Verify and commit only the task's files on the feature branch.
- Before merging back, update against the latest target branch and rerun the relevant checks.
- Merge only when the target worktree is safe. If it is still dirty or the merge would overwrite parallel work, report the blocker instead of forcing the merge.
- After a successful merge into `custom`, remove the corresponding temporary worktree and delete the merged local feature branch in the same task. Never delete `main`, `custom`, remote branches, or any branch checked out by an active worktree. Do not push unless the user asks.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
