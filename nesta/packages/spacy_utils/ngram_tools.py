def remove_stop_word_ngrams(ls, noise):
    """Function to remove frequent/stopword-like ngrams. Applied after creating ngrams (bigram and over) from document tokens.

    Args:

    ls (list): List of document ngram tokens

    noise (noise): List of ngrams to remove (i.e. stopword list)

    Returns:
        cleaned_token_list (list): List of tokens without stopwords

    """

    return [i for i in ls if i not in noise]
