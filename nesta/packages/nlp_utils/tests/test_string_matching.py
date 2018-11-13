from unittest import TestCase
from nesta.packages.nlp_utils.string_matching import knuth_morris_pratt


TEST_DOC = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum xxx."


class TestKMP(TestCase):

    def test_str(self):
        n = 0
        for result in knuth_morris_pratt(TEST_DOC, "in"):
            n += 1
        assert n == 7

    def test_list(self):
        n = 0
        for result in knuth_morris_pratt(TEST_DOC.split(), ["in"]):
            n += 1
        assert n == 3

    def test_overlap(self):
        n = 0
        for result in knuth_morris_pratt(TEST_DOC, "xx", overlaps=True):
            n += 1
        assert n == 2
