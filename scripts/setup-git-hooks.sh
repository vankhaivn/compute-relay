#!/usr/bin/env sh
set -eu

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

git config core.hooksPath .githooks
if [ "$(git config --local --get commit.template || true)" = .gitmessage ]; then
  git config --local --unset-all commit.template
fi

printf '%s\n' 'Configured core.hooksPath=.githooks'
