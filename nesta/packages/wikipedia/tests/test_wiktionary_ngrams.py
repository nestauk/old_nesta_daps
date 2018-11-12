from nesta.packages.wikipedia.wiktionary_ngrams import find_latest_wikidump
from nesta.packages.wikipedia.wiktionary_ngrams import extract_ngrams
from unittest import TestCase


class TestWiktionaryNgrams(TestCase):
    def test_pipeline(self):
        date = find_latest_wikidump()
        self.assertIsNotNone(date)
        ngrams = extract_ngrams(date)
        self.assertGreater(len(ngrams), 1e5)
