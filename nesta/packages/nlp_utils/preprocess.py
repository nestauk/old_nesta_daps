import re
import string
import gensim
import nltk
from nltk.corpus import stopwords
import numpy as np
from operator import iadd
from functools import reduce
from sklearn.feature_extraction.text import TfidfVectorizer
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

stop_words = set(stopwords.words('english') +
                 list(string.punctuation) +
                 ['\\n'] + ['quot'])

regex_str = [r"http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|"
             r"[!*\(\),](?:%[0-9a-f][0-9a-f]))+",
             r"(?:\w+-\w+){2}",
             r"(?:\w+-\w+)",
             r"(?:\\\+n+)",
             r"(?:@[\w_]+)",
             r"<[^>]+>",
             r"(?:\w+'\w)",
             r"(?:[\w_]+)",
             r"(?:\S)"]

# Create the tokenizer which will be case insensitive and will ignore space.
tokens_re = re.compile(r'('+'|'.join(regex_str)+')',
                       re.VERBOSE | re.IGNORECASE)


def tokenize_document(text, remove_stops=False):
    """Preprocess a whole raw document.
    Args:
        text (str): Raw string of text.
        remove_stops (bool): Flag to remove english stopwords
    Return:
        List of preprocessed and tokenized documents
    """
    return [clean_and_tokenize(sentence, remove_stops)
            for sentence in nltk.sent_tokenize(text)]


def clean_and_tokenize(text, remove_stops):
    """Preprocess a raw string/sentence of text.
    Args:
       text (str): Raw string of text.
       remove_stops (bool): Flag to remove english stopwords
    Return:
       tokens (list, str): Preprocessed tokens.
    """

    tokens = tokens_re.findall(text)
    _tokens = [t.lower() for t in tokens]
    filtered_tokens = [token.replace('-', '_') for token in _tokens
                       if len(token) > 2
                       and (not remove_stops or token not in stop_words)
                       and not any(x in token for x in string.digits)
                       and any(x in token for x in string.ascii_lowercase)]
    return filtered_tokens


def filter_by_idf(documents, lower_idf_limit, upper_idf_limit):
    """Remove (from documents) terms which are in a range of IDF values.
    
    Args:
        documents (list): Either a :obj:`list` of :obj:`str` or a
                          :obj:`list` of :obj:`list` of :obj:`str` to be
                          filtered.
        lower_idf_limit (float): Lower percentile (between 0 and 100) on which
                                 to exclude terms by their IDF.
        upper_idf_limit (float): Upper percentile (between 0 and 100) on which
                                 to exclude terms by their IDF.

    Returns:
        Filtered documents
    """
    # Check the shape of the input documents
    docs = documents
    if type(documents[0]) is list:
        docs = [reduce(iadd, d) for d in documents]
    # Evaluate the TFIDF
    tfidf = TfidfVectorizer(tokenizer=lambda x: x, lowercase=False)
    tfidf.fit(docs)
    lower_idf = np.percentile(tfidf.idf_, lower_idf_limit)
    upper_idf = np.percentile(tfidf.idf_, upper_idf_limit)
    # Pick out the vocab to be dropped
    drop_vocab = set(term for term, idx in tfidf.vocabulary_.items()
                     if tfidf.idf_[idx] < lower_idf
                     or tfidf.idf_[idx] >= upper_idf)
    # Filter the documents
    new_docs = []
    for doc in documents:
        _new_doc = []
        for sent in doc:
            _new_sent = [w for w in sent if w not in drop_vocab]
            if len(_new_sent) == 0:
                continue
            _new_doc.append(_new_sent)
        new_docs.append(_new_doc)
    return new_docs


if __name__ == '__main__':
    nltk.download("gutenberg")
    from nltk.corpus import gutenberg
    docs = []
    for fid in gutenberg.fileids():
        f = gutenberg.open(fid)
        docs.append(f.read())
        f.close()
    docs = [tokenize_document(d) for d in docs]
    docs = filter_by_idf(docs, 10, 90)
