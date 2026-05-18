#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_UNDER_TEST="${REPO_ROOT}/scripts/pre-commit/forbid-prompts-files.sh"
TMP_DIR="$(mktemp -d)"
failures=0

trap 'rm -rf "${TMP_DIR}"' EXIT

init_repo() {
  local repo="$1"
  mkdir -p "${repo}"
  git -C "${repo}" init -q
  git -C "${repo}" config user.email "pre-commit-test@example.invalid"
  git -C "${repo}" config user.name "Pre Commit Test"
}

run_hook_capture() {
  local repo="$1"
  local output_file="$2"

  (cd "${repo}" && "${SCRIPT_UNDER_TEST}") >"${output_file}" 2>&1
}

assert_output_contains() {
  local output_file="$1"
  local needle="$2"

  if ! grep -Fq -- "${needle}" "${output_file}"; then
    echo "Expected hook output to contain: ${needle}" >&2
    echo "Actual output:" >&2
    sed 's/^/  | /' "${output_file}" >&2
    return 1
  fi
}

assert_hook_succeeds() {
  local repo="$1"
  local output_file="$2"

  if ! run_hook_capture "${repo}" "${output_file}"; then
    echo "Expected hook to succeed, but it failed." >&2
    sed 's/^/  | /' "${output_file}" >&2
    return 1
  fi
}

assert_hook_fails_with_prompt_warning() {
  local repo="$1"
  local output_file="$2"
  local expected_path="$3"

  if run_hook_capture "${repo}" "${output_file}"; then
    echo "Expected hook to fail, but it succeeded." >&2
    return 1
  fi

  assert_output_contains "${output_file}" "DO NOT COMMIT prompts/ FILES" || return 1
  assert_output_contains "${output_file}" "under no circumstances" || return 1
  assert_output_contains "${output_file}" "human operator directly and explicitly instructed" || return 1
  assert_output_contains "${output_file}" "${expected_path}" || return 1
}

run_test() {
  local name="$1"
  shift

  if "$@"; then
    echo "ok - ${name}"
  else
    echo "not ok - ${name}" >&2
    failures=$((failures + 1))
  fi
}

test_allows_non_prompts_changes() {
  local repo="${TMP_DIR}/non-prompts"
  local output_file="${TMP_DIR}/non-prompts.out"

  init_repo "${repo}"
  mkdir -p "${repo}/src"
  printf 'safe\n' >"${repo}/src/file.txt"
  git -C "${repo}" add src/file.txt

  assert_hook_succeeds "${repo}" "${output_file}"
}

test_blocks_force_added_ignored_prompts_file() {
  local repo="${TMP_DIR}/force-added"
  local output_file="${TMP_DIR}/force-added.out"

  init_repo "${repo}"
  printf 'prompts/\n' >"${repo}/.gitignore"
  git -C "${repo}" add .gitignore
  git -C "${repo}" commit -q -m "ignore prompts"

  mkdir -p "${repo}/prompts"
  printf 'operator scratchpad\n' >"${repo}/prompts/plan.md"
  git -C "${repo}" add -f prompts/plan.md

  assert_hook_fails_with_prompt_warning "${repo}" "${output_file}" "prompts/plan.md"
}

test_blocks_tracked_prompts_modification() {
  local repo="${TMP_DIR}/tracked-modification"
  local output_file="${TMP_DIR}/tracked-modification.out"

  init_repo "${repo}"
  mkdir -p "${repo}/prompts"
  printf 'initial\n' >"${repo}/prompts/TODO.md"
  git -C "${repo}" add prompts/TODO.md
  git -C "${repo}" commit -q -m "seed tracked prompt"

  printf 'modified\n' >>"${repo}/prompts/TODO.md"
  git -C "${repo}" add prompts/TODO.md

  assert_hook_fails_with_prompt_warning "${repo}" "${output_file}" "prompts/TODO.md"
}

test_blocks_prompts_deletion() {
  local repo="${TMP_DIR}/deletion"
  local output_file="${TMP_DIR}/deletion.out"

  init_repo "${repo}"
  mkdir -p "${repo}/prompts"
  printf 'initial\n' >"${repo}/prompts/old.md"
  git -C "${repo}" add prompts/old.md
  git -C "${repo}" commit -q -m "seed tracked prompt"

  rm "${repo}/prompts/old.md"
  git -C "${repo}" add -u prompts/old.md

  assert_hook_fails_with_prompt_warning "${repo}" "${output_file}" "prompts/old.md"
}

run_test "allows staged files outside prompts/" test_allows_non_prompts_changes
run_test "blocks force-added ignored prompts/ files" test_blocks_force_added_ignored_prompts_file
run_test "blocks modifications to already tracked prompts/ files" test_blocks_tracked_prompts_modification
run_test "blocks deletions under prompts/" test_blocks_prompts_deletion

if (( failures > 0 )); then
  exit 1
fi
