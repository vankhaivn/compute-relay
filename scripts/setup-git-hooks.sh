#!/usr/bin/env sh
set -eu

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

git config core.hooksPath .githooks
git config commit.template .gitmessage

printf '%s\n' 'Configured core.hooksPath=.githooks'
printf '%s\n' 'Configured commit.template=.gitmessage'
