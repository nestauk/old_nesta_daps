from nlp_utils.preprocess import tokenize_document
from nlp_utils.preprocess import build_ngrams
import nltk
from nltk.corpus import gutenberg
import unittest

class PreprocessTest(unittest.TestCase):
    
    def setUp(self):
        nltk.download("gutenberg")
        self.docs = []
        for fid in gutenberg.fileids():
            f = gutenberg.open(fid)
            self.docs.append(f.read())
            f.close()

    def test_ngrams(self):
        docs = [tokenize_document(d) for d in self.docs]
        docs = build_ngrams(docs, n=3)
        n = sum("poor_miss_taylor" in sent 
                for sent in docs[0])
        self.assertGreater(n, 0)


if __name__ == "__main__":
    unittest.main()
