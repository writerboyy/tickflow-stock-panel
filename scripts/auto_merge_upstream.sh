#!/usr/bin/env bash
#
# auto_merge_upstream.sh
# ---------------------------------------------------------------
# 每天自动把 upstream 最新代码合并到本仓库的 custom 分支：
#   - 冲突时完全采用 upstream 版本（git merge -X theirs）
#   - 合并完成后推送到 origin/custom
#   - 运行前若工作区有未提交改动，先 git stash 暂存，结束后原样恢复
#
# 用法:
#   bash scripts/auto_merge_upstream.sh
#
set -uo pipefail

# 仓库根目录（脚本位于 <repo>/scripts/ 下）
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR" || { echo "无法进入仓库目录: $REPO_DIR"; exit 1; }

UPSTREAM_REMOTE="upstream"
UPSTREAM_BRANCH="main"
TARGET_BRANCH="custom"
ORIGIN_REMOTE="origin"

LOG_DIR="$REPO_DIR/.workbuddy"
LOG_FILE="$LOG_DIR/auto_merge_upstream.log"
mkdir -p "$LOG_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# 恢复现场（切回原分支 + 还原 stash）后退出
restore() {
  local rc=$1
  if [ -n "${BRANCH_BEFORE:-}" ] && [ "$BRANCH_BEFORE" != "$TARGET_BRANCH" ]; then
    if git switch "$BRANCH_BEFORE" 2>/dev/null; then
      log "已切回原分支 $BRANCH_BEFORE"
    else
      log "⚠️ 切回 $BRANCH_BEFORE 失败，请手动处理"
    fi
  fi
  if [ "${STASHED:-0}" = "1" ]; then
    log "恢复 stash（git stash pop）..."
    if git stash pop; then
      log "stash 已恢复，工作区改动已还原"
    else
      log "⚠️ stash pop 冲突：stash 已保留（git stash list 查看），请手动解决"
    fi
  fi
  log "===== 结束（退出码 $rc）====="
  exit "$rc"
}

log "===== 开始 upstream($UPSTREAM_REMOTE/$UPSTREAM_BRANCH) → $TARGET_BRANCH 自动合并 ====="

BRANCH_BEFORE="$(git symbolic-ref --short HEAD 2>/dev/null || echo '')"
log "运行前分支: ${BRANCH_BEFORE:-<detached>}"

# 1) 检测未提交改动（含未跟踪文件），有则先 stash
if [ -n "$(git status --porcelain)" ]; then
  STASH_MSG="auto-upstream-merge $(date '+%Y%m%d-%H%M%S')"
  log "检测到未提交改动，先 stash（msg: $STASH_MSG）"
  git stash push -u -m "$STASH_MSG"
  STASHED=1
else
  STASHED=0
  log "工作区干净，无需 stash"
fi

# 2) 切到目标分支
if [ "$BRANCH_BEFORE" != "$TARGET_BRANCH" ]; then
  if git switch "$TARGET_BRANCH" 2>/dev/null; then
    log "已切换到 $TARGET_BRANCH"
  else
    log "无法切换到 $TARGET_BRANCH，终止"
    restore 1
  fi
fi

# 3) 拉取 upstream 最新
log "git fetch $UPSTREAM_REMOTE ..."
if ! git fetch "$UPSTREAM_REMOTE"; then
  log "⚠️ fetch $UPSTREAM_REMOTE 失败（可能无网络/无权限）"
  restore 1
fi

if ! git rev-parse --verify --quiet "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH" >/dev/null; then
  log "未找到 $UPSTREAM_REMOTE/$UPSTREAM_BRANCH，终止"
  restore 1
fi

# 4) 合并（冲突完全取上游）
BEFORE="$(git rev-parse HEAD)"
INCOMING="$(git rev-list --count "$BEFORE..$UPSTREAM_REMOTE/$UPSTREAM_BRANCH" 2>/dev/null || echo 0)"
log "待合并的 upstream 新提交数: $INCOMING"

if git merge -X theirs --no-edit "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH"; then
  AFTER="$(git rev-parse HEAD)"
  if [ "$BEFORE" = "$AFTER" ]; then
    log "$TARGET_BRANCH 已与 upstream 同步，无新合并"
  else
    log "合并完成 ✅（新增/变更提交数: $INCOMING）"
  fi
else
  log "⚠️ 合并失败，执行 git merge --abort"
  git merge --abort 2>/dev/null
  restore 1
fi

# 5) 推送到 origin
log "git push $ORIGIN_REMOTE $TARGET_BRANCH ..."
if git push "$ORIGIN_REMOTE" "$TARGET_BRANCH"; then
  log "推送成功 → $ORIGIN_REMOTE/$TARGET_BRANCH ✅"
else
  log "⚠️ 推送失败（可能无凭证/网络），本地合并已完成，请手动 push"
fi

restore 0
