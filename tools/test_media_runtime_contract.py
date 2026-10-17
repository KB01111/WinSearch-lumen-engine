"""Malformed worker replies must fail the real-media acceptance helper."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('media_runtime', Path(__file__).with_name('test-media-runtime.py'))
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class ReplyContract(unittest.TestCase):
    def reply(self, count):
        return {'type': 'embeddings', 'stub': False, 'backend': 'cpu', 'dim': 768,
                'vecs': [[1.0] + [0.0] * 767 for _ in range(count)]}

    def test_missing_or_extra_media_vectors_are_rejected(self):
        for count in [0, 2]:
            with self.subTest(count=count), self.assertRaises(AssertionError):
                runtime.validate_vectors(self.reply(count), 1)

    def test_text_batch_must_preserve_arity(self):
        with self.assertRaises(AssertionError):
            runtime.validate_vectors(self.reply(1), 2)

    def test_correct_media_and_batch_counts_pass(self):
        for count in [1, 4]:
            self.assertEqual(len(runtime.validate_vectors(self.reply(count), count)), count)


if __name__ == '__main__':
    unittest.main()
