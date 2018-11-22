from unittest import TestCase
from nesta.packages.nlp_utils.ngrammer import Ngrammer


class TestNgrammer(TestCase):
    def test_ngrammer(self):
        ngrammer = Ngrammer(database="production_tests")
        ngrammer.ngrams.clear()
        ngrammer.ngrams[3].add('convolutional_neural_networks')
        ngrammer.ngrams[3].add('bed_and_breakfast')
        ngrammer.ngrams[2].add('neural_networks')
        document = ("This is a document about machine "
                    "learning, convolutional neural networks, "
                    "neural networks and bed and breakfast")
        processed_doc = ngrammer.process_document(document)
        for _, ngrams in ngrammer.ngrams.items():
            for ng in ngrams:
                self.assertIn(ng, processed_doc[0])
