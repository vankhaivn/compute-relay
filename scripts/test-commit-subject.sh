#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
validator="$script_dir/check-commit-subject.sh"
checks=0

expect() {
  expected=$1
  subject=$2
  status=0
  output=$(sh "$validator" "$subject" 2>&1) || status=$?
  if [ "$status" -ne "$expected" ]; then
    printf 'Expected exit %s, got %s for: %s\n%s\n' "$expected" "$status" "$subject" "$output" >&2
    exit 1
  fi
  checks=$((checks + 1))
}

# Test the full subject length, including type and scope, using ASCII fixtures.
for length in 72 73 80 81 100; do
  subject='test(repo): '
  while [ "${#subject}" -lt "$length" ]; do
    subject="${subject}x"
  done
  if [ "$length" -le 80 ]; then
    expect 0 "$subject"
  else
    expect 1 "$subject"
    case "$output" in
      *"maximum is 80."*) ;;
      *) printf '%s\n' 'Missing current length limit in diagnostic.' >&2; exit 1 ;;
    esac
  fi
done

# The previously blocked 73-character subject is valid without history edits.
expect 0 'test(kaggle): exercise staging process framing and finite payload streams'
expect 0 'feat: add staging'
expect 0 'feat(api)!: require explicit attempt identity'
expect 0 "Merge branch 'fixture'"
expect 0 'Revert "feat: add staging"'

# Formatting has its own conventional type; no other syntax rule is bypassed.
expect 0 'style(acceptance): format the resumed model and adapter foundation'
expect 0 'style: normalize whitespace'
expect 1 'styles: normalize whitespace'
expect 1 'style: Normalize whitespace'
expect 1 'style: normalize whitespace.'

# Changing the length ceiling must not relax the other existing syntax rules.
expect 1 ''
expect 1 'unknown(repo): add staging'
expect 1 'feat(repo): Add staging'
expect 1 'feat(BadScope): add staging'
expect 1 'feat(repo):add staging'
expect 1 'fix(repo): correct staging.'

printf 'commit-subject: %s regression checks passed\n' "$checks"
