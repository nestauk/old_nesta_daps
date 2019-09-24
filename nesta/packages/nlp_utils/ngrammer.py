"""
ngrammer
========

Find and replace n-grams in text based on
previously collected wiktionary n-grams.
"""

import os
from sqlalchemy.orm import sessionmaker
from collections import defaultdict

from nesta.core.orms.wiktionary_ngrams_orm import WiktionaryNgram
from nesta.core.orms.wiktionary_ngrams_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.packages.nlp_utils.preprocess import tokenize_document
from nesta.packages.nlp_utils.preprocess import stop_words


class Ngrammer:
    """Find and replace n-grams in text based on
    previously collected wiktionary n-grams.

    Args:
        config_filepath (str): Path to the db configuration file.
                  If not specified, it looks instead for the environ
                  varialble 'MYSQLDBCONF'
        database (str): Database name
    """
    def __init__(self, config_filepath=None, database="dev"):
        if config_filepath is not None:
            os.environ["MYSQLDBCONF"] = config_filepath
        engine = get_mysql_engine("MYSQLDBCONF", "mysqldb",
                                  database=database)
        Session = sessionmaker(engine)
        Base.metadata.create_all(engine)
        session = Session()
        # Split out n-grams by size (speeds up the extraction later)
        self.ngrams = defaultdict(set)
        for row in session.query(WiktionaryNgram).all():
            size = row.ngram.count("_") + 1
            self.ngrams[size].add(row.ngram)

    def find_and_replace(self, sentence, size):
        """Find and replace any n-grams of :obj:`size`. Stops if a single
        n-gram is found (to avoid 'changed size during iteration'
        exceptions). Therefore you would need
        to run recursively to find and replace throughout a sentence

        Args:
             sentence (str): Sentence to scan for n-grams.
             size (int): N-gram size to consider
        Returns:
             success (bool): Whether or not an n-gram was found.
        """
        # Iterate over ngrams in the sentence
        for loc, ngram in enumerate(zip(*[sentence[i:] for i in range(size)])):
            # Query existence of the ngram in the official n-gram list
            joined_ngram = "_".join(ngram)
            if joined_ngram not in self.ngrams[size]:
                continue
            # Remove the n-gram components
            for i in range(size-1):
                del sentence[loc]
            # Then add the "joined" n-gram in it's place
            sentence[loc] = joined_ngram
            return True
        return False

    def process_document(self, raw_text, remove_stops=True):
        """Tokenize and insert n-grams into documents.

        Args:
             raw_text (str): A raw text document.
             remove_stops (bools): Whether or not to remove stops.
        Returns:
             processed_doc (list): Iterable ready for word embedding
        """
        # Tokenize and clean up the text first
        text = tokenize_document(raw_text)
        # Replace large n-grams first, then small n-grams
        for size in sorted(self.ngrams, reverse=True):
            for sentence in text:
                # Ignore n-grams longer than the sentence(!)
                if size > len(sentence):
                    continue
                # Replace until done
                while self.find_and_replace(sentence, size):
                    pass

        # Remove stop words if required
        processed_doc = text
        if remove_stops:
            processed_doc = []
            for sentence in text:
                sentence = [token for token in sentence
                            if token not in stop_words]
                processed_doc.append(sentence)
        return processed_doc


if __name__ == "__main__":
    ngrammer = Ngrammer("/Users/jklinger/Nesta-AWS/AWS-RDS-config/"
                        "innovation-mapping-5712.config")
    document = ("This is a document about machine "
                "learning, convolutional neural networks, "
                "neural networks and bed and breakfast")
    processed_doc = ngrammer.process_document(document)
    print(repr(document), "becomes", processed_doc, sep="\n")
