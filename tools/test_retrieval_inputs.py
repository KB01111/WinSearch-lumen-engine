from pathlib import Path
import tempfile
import unittest

from retrieval_inputs import verify_fixture


class FrozenFixtureTests(unittest.TestCase):
    def test_windows_eol_conversion_preserves_frozen_bytes_but_content_edits_fail(self):
        source = Path(__file__).resolve().parents[1] / 'evals/retrieval-development.json'
        canonical = source.read_bytes().replace(b'\r\n', b'\n')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fixture.json'
            path.write_bytes(canonical)
            expected = verify_fixture(path, 'retrieval-development')
            path.write_bytes(canonical.replace(b'\n', b'\r\n'))
            self.assertEqual(verify_fixture(path, 'retrieval-development'), expected)
            path.write_bytes(canonical + b' ')
            with self.assertRaisesRegex(ValueError, 'Frozen fixture changed'):
                verify_fixture(path, 'retrieval-development')

    def test_all_current_fixtures_match_canonical_pins(self):
        from retrieval_inputs import FROZEN
        root = Path(__file__).resolve().parents[1]
        for suite in FROZEN:
            with self.subTest(suite=suite):
                self.assertEqual(verify_fixture(root / 'evals' / (suite + '.json'), suite), FROZEN[suite])
