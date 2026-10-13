"""Select extra Go checks; ordinary Linux checks always run."""

import os
import re
import subprocess


LIGHT_PREFIXES = (
    "api/", "docs/", "examples/", "internal/contracts/", "internal/appcli/",
    "internal/operatorcli/", "internal/cli/", "internal/bundlectl/",
    "internal/companion/", "internal/packaging/", "internal/buildinfo/",
    "cmd/contractcheck/", "cmd/companionpack/",
)
PLATFORM_PREFIXES = (
    "internal/blobfs/", "internal/statefs/", "internal/safefs/",
    "internal/credentials/", "internal/localinput/", "internal/store/",
    "internal/companion/", "internal/packaging/", "internal/bundlectl/",
    "internal/cli/", "internal/appcli/", "internal/operatorcli/",
    "internal/provider/kaggle/", "cmd/", "scripts/package-companion",
)


def classify(paths, event):
    if event != "pull_request" or not paths:
        return True, True
    race = macos = False
    for path in paths:
        # Changing the selection, commands or dependencies must exercise both lanes.
        if path in ("go.mod", "go.sum") or path.startswith(
            (".github/", "scripts/dev.", "internal/devtool/")
        ):
            return True, True
        if path.endswith(".md"):
            continue
        race |= not path.startswith(LIGHT_PREFIXES)
        macos |= path.startswith(PLATFORM_PREFIXES) or bool(
            re.search(r"_(darwin|unix|other|windows)(?:_test)?\.go$", path)
        )
        # Go code outside the usual source roots runs both lanes.
        if path.endswith(".go") and not path.startswith(("internal/", "cmd/", "runner/")):
            race = macos = True
    return race, macos


def changed_paths(base, head):
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("CI diff requires full commit IDs")
    # Include both sides of renames and deletions, without newline/path quoting ambiguity.
    raw = subprocess.check_output([
        "git", "diff", "--name-only", "--no-renames", "-z", f"{base}...{head}", "--",
    ])
    return [os.fsdecode(path) for path in raw.split(b"\0") if path]


def main():
    event = os.environ["CI_EVENT_NAME"]
    paths = changed_paths(os.environ["CI_BASE_SHA"], os.environ["CI_HEAD_SHA"]) if event == "pull_request" else []
    race, macos = classify(paths, event)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"race={str(race).lower()}\nmacos={str(macos).lower()}\n")
    print(f"Extra Go checks: race={race}, macOS={macos}")


if __name__ == "__main__":
    main()
