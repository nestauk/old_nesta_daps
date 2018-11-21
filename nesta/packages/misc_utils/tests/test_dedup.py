from unittest import TestCase
from nesta.packages.misc_utils.dedup import dedup

class TestDedup(TestCase):

    def test_dedup(self):
        docs = {1: "aaab", 2: "aaa", 3: "aaac", 4: "aaaaaaaaaaaaaa",
                5: "aaa", 6: "aaac"}
        deduped = dedup(docs)
        self.assertEqual(deduped[1], 1)
        self.assertEqual(deduped[2], 2)
        self.assertEqual(deduped[3], 3)
        self.assertEqual(deduped[4], 4)
        self.assertEqual(deduped[5], 2)
        self.assertEqual(deduped[6], 3)

    def test_baddocs(self):
        docs = {1: "", 2: "", 3: "aaaa"}
        deduped = dedup(docs)
        self.assertEqual(deduped[1], 1)
        self.assertEqual(deduped[2], 1)
        self.assertEqual(deduped[3], 3)

    def test_nodocs(self):
        docs = {}
        deduped = dedup(docs)
        self.assertEqual(deduped, docs)
