import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ci_scope import changed_paths, classify


class ScopeTests(unittest.TestCase):
    def test_runtime_changes_keep_race(self):
        for path in (
            "internal/store/sqlite/migrations/0022.sql", "internal/scheduler/queue.go",
            "internal/dispatch/worker.go", "internal/runtimehost/runtime.go",
            "internal/api/server.go", "internal/provider/kaggle/cleanup.py",
            "internal/provider/sshhost/image/Dockerfile", "internal/domain/job.go",
            "internal/ports/store.go", "runner/python/relay_runner/main.py",
            "internal/newpackage/new.go",
        ):
            with self.subTest(path=path):
                self.assertTrue(classify([path], "pull_request")[0])

    def test_light_changes_and_mixed_pr(self):
        self.assertEqual(classify(["internal/contracts/check.go", "docs/usage.md"], "pull_request"), (False, False))
        self.assertEqual(classify(["internal/appcli/help.go"], "pull_request"), (False, True))
        self.assertEqual(classify(["docs/usage.md", "internal/store/sqlite/store.go"], "pull_request"), (True, True))

    def test_platform_and_policy_changes(self):
        for path in (
            "internal/credentials/file.go", "internal/safefs/root_linux.go",
            "internal/statefs/lock.go", "internal/blobfs/file.go", "internal/store/sqlite/store.go",
            "internal/newpackage/file_darwin_test.go", "newpackage/file.go",
            ".github/workflows/go.yml", ".github/scripts/ci_scope.py", "go.sum",
            "go.mod", "scripts/dev.ps1", "internal/devtool/devtool.go",
        ):
            with self.subTest(path=path):
                self.assertEqual(classify([path], "pull_request"), (True, True))

    def test_main_and_unknown_events_are_full(self):
        for event in ("push", "workflow_dispatch", "unknown"):
            self.assertEqual(classify(["docs/usage.md"], event), (True, True))
        self.assertEqual(classify([], "pull_request"), (True, True))

    def test_bad_diff_is_not_an_empty_pass(self):
        with self.assertRaises(ValueError):
            changed_paths("--help", "a" * 40)
        with patch("ci_scope.subprocess.check_output", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                changed_paths("a" * 40, "b" * 40)

    def test_rename_preserves_deleted_core_path(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args], stderr=subprocess.DEVNULL).decode().strip()

            git("init", "-q")
            git("config", "user.name", "CI fixture")
            git("config", "user.email", "fixture@example.invalid")
            old = Path(directory, "internal/store/sqlite/old.go")
            old.parent.mkdir(parents=True)
            old.write_text("package fixture\n")
            git("add", ".")
            git("commit", "-qm", "test: initial fixture")
            base = git("rev-parse", "HEAD")
            new = Path(directory, "docs/renamed\nfile.md")
            new.parent.mkdir()
            old.rename(new)
            git("add", "-A")
            git("commit", "-qm", "test: rename fixture")
            head = git("rev-parse", "HEAD")
            previous = os.getcwd()
            try:
                os.chdir(directory)
                paths = changed_paths(base, head)
            finally:
                os.chdir(previous)
            self.assertEqual(set(paths), {"internal/store/sqlite/old.go", "docs/renamed\nfile.md"})
            self.assertEqual(classify(paths, "pull_request"), (True, True))


if __name__ == "__main__":
    unittest.main()
