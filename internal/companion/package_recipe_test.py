"""Regression checks for deterministic wheel metadata after launcher removal."""
import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "package_companion", Path(__file__).resolve().parents[2] / "scripts/package-companion.py"
)
RECIPE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECIPE)


class WheelRecordTest(unittest.TestCase):
    def test_build_specific_launcher_rows_are_removed_and_other_rows_preserved(self):
        results = []
        with tempfile.TemporaryDirectory() as directory:
            for index in range(2):
                site = Path(directory) / str(index)
                (site / "bin").mkdir(parents=True)
                (site / "bin/tool").write_text("#!/different-build-path-" + str(index))
                (site / "package.dist-info").mkdir()
                record = site / "package.dist-info/RECORD"
                stable = [["package/module.py", "sha256=preserved", "42"],
                          ["package.dist-info/RECORD", "", ""],
                          ["package.dist-info/licenses/LICENSE", "sha256=license", "7"]]
                with record.open("w", newline="") as stream:
                    csv.writer(stream).writerows([["bin/tool", "sha256=build-" + str(index), "123"], *stable])
                RECIPE.remove_console_scripts(site)
                self.assertFalse((site / "bin").exists())
                with record.open(newline="") as stream:
                    self.assertEqual(list(csv.reader(stream)), stable)
                results.append(record.read_bytes())
        self.assertEqual(results[0], results[1])

    def test_escaping_or_noncanonical_records_fail_before_mutating_staging(self):
        for path in ("../escape", "bin/../escape", "/absolute", "bin//tool", "bin/./tool", "bin\\tool", "C:/tool"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                site = Path(directory)
                (site / "bin").mkdir()
                launcher = site / "bin/tool"
                launcher.write_text("keep until validation")
                (site / "package.dist-info").mkdir()
                record = site / "package.dist-info/RECORD"
                with record.open("w", newline="") as stream:
                    csv.writer(stream).writerows([[path, "sha256=unknown", "1"], ["package.dist-info/RECORD", "", ""]])
                before = record.read_bytes()
                with self.assertRaisesRegex(RuntimeError, "unsafe installed wheel RECORD path"):
                    RECIPE.remove_console_scripts(site)
                self.assertTrue(launcher.exists())
                self.assertEqual(record.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
