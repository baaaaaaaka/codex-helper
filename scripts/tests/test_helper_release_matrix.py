import importlib.util
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "ci" / "helper_release_matrix.py"
SPEC = importlib.util.spec_from_file_location("helper_release_matrix", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class HelperReleaseMatrixTests(unittest.TestCase):
    def test_selects_every_formal_release_at_or_after_minimum(self):
        releases = [
            {"tagName": "v0.0.37", "isPrerelease": False, "isDraft": False},
            {"tagName": "v0.0.38", "isPrerelease": False, "isDraft": False},
            {"tagName": "v0.1.0-rc.1", "isPrerelease": True, "isDraft": False},
            {"tagName": "v0.1.0", "isPrerelease": False, "isDraft": False},
            {"tagName": "v0.1.1", "isPrerelease": False, "isDraft": True},
            {"tagName": "v0.1.2", "isPrerelease": False, "isDraft": False},
        ]
        self.assertEqual(
            MODULE.select_tags(releases, "v0.0.38", "v0.1.2"),
            ["v0.0.38", "v0.1.0"],
        )

    def test_rejects_non_formal_minimum(self):
        with self.assertRaises(ValueError):
            MODULE.select_tags([], "v0.1.0-rc.1", "")

    def test_rejects_matrix_that_would_exceed_github_limit(self):
        with self.assertRaisesRegex(ValueError, "split the workflow by platform"):
            MODULE.validate_matrix_size([f"v0.1.{value}" for value in range(43)], 6, 256)

    def test_accepts_current_six_target_capacity(self):
        MODULE.validate_matrix_size([f"v0.1.{value}" for value in range(42)], 6, 256)


if __name__ == "__main__":
    unittest.main()
