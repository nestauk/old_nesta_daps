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

regex_str = ["http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|"
             "[!*\(\),](?:%[0-9a-f][0-9a-f]))+",
             "(?:\w+-\w+){2}",
             "(?:\w+-\w+)",
             "(?:\\\+n+)",
             "(?:@[\w_]+)",
             "<[^>]+>",
             "(?:\w+'\w)",
             "(?:[\w_]+)",
             "(?:\S)"]

# Create the tokenizer which will be case insensitive and will ignore space.
tokens_re = re.compile(r'('+'|'.join(regex_str)+')',
                       re.VERBOSE | re.IGNORECASE)


def tokenize_document(text):
    """Preprocess a whole raw document.
    Args:
        text (str): Raw string of text.
    Return:
        List of preprocessed and tokenized documents
    """
    return [clean_and_tokenize(sentence)
            for sentence in nltk.sent_tokenize(text)]


def clean_and_tokenize(text):
    """Preprocess a raw string/sentence of text.
    Args:
       text (str): Raw string of text.
    Return:
       tokens (list, str): Preprocessed tokens.
    """

    tokens = tokens_re.findall(text)
    _tokens = [t.lower() for t in tokens]
    filtered_tokens = [token.replace('-', '_') for token in _tokens
                       if len(token) > 2
                       and token not in stop_words
                       and not any(x in token for x in string.digits)
                       and any(x in token for x in string.ascii_lowercase)]
    return filtered_tokens


def build_ngrams(documents, n=2, **kwargs):
    """Create ngrams using Gensim's phrases.
    Args:
        documents (:obj:`list` of token lists): List of preprocessed and
                                                tokenized documents
        n (int): The `n` in n-gram.
    """
    # Check whether "level" was passed as an argument
    if "level" not in kwargs:
        level = 2
    else:
        level = kwargs["level"]
    # Generate sentences, as required for gensim Phrases
    sentences = []
    for doc in documents:
        sentences += doc
    # Get the bigrams
    phrases = gensim.models.Phrases(sentences, min_count=2, delimiter=b'_')
    bigram = gensim.models.phrases.Phraser(phrases)
    docs_bi = [[bigram[sentence] for sentence in doc] for doc in documents]
    # If finished
    if level == n:
        return docs_bi
    # Otherwise, keep processing until n-grams satisfied
    return build_ngrams(docs_bi, n=n, level=level+1)


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
    docs = build_ngrams(docs)
    docs = filter_by_idf(docs, 10, 90)
