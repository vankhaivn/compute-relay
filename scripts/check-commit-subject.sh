#!/usr/bin/env sh
set -eu

subject=${1:-}
max_subject_length=80
pattern='^(feat|fix|docs|refactor|perf|test|build|ci|chore|style|revert)(\([a-z0-9][a-z0-9._/-]*\))?(!)?: [a-z0-9].*$'

fail() {
  printf '%s\n' "Invalid commit subject: $subject" >&2
  printf '%s\n' 'Expected: <type>(<scope>)!: <lowercase summary>' >&2
  printf '%s\n' 'Types: feat, fix, docs, refactor, perf, test, build, ci, chore, style, revert' >&2
  printf '%s\n' 'See docs/development/commit-convention.md' >&2
  exit 1
}

[ -n "$subject" ] || fail

case "$subject" in
  Merge\ *|Revert\ \"*\") exit 0 ;;
esac

printf '%s\n' "$subject" | grep -Eq "$pattern" || fail

[ "${#subject}" -le "$max_subject_length" ] || {
  printf '%s\n' "Commit subject is ${#subject} characters; maximum is $max_subject_length." >&2
  exit 1
}

case "$subject" in
  *.)
    printf '%s\n' 'Commit subject must not end with a period.' >&2
    exit 1
    ;;
esac
