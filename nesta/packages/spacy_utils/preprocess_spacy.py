import spacy
nlp = spacy.load('en', disable=['ner'])

def convert(text):

    """Function to remove/replace curly quotes (“, ”, ‘, ’)

    Args:
        text (str): String of document.

    Returns:
        transformed_doc (str): Curly quotes replaced/removed in document.

    """

    return str(text).replace('“',"").replace('”',"").replace('‘',"'").replace('’',"'")

def spacy_nlp_vocab_update(stop_word_list):
    """Function to update Spacy's NLP vocabulary

    Args:
        stop_word_list (list): List of stop-words.

    """

    for word in stop_word_list:
        l = word.lower()
        u = word.upper()
        t = word.title()
        nlp.vocab[l].is_stop = True
        nlp.vocab[u].is_stop = True
        nlp.vocab[t].is_stop = True

def word_tokenise(text):
    """Preprocessing text using spacy library

    Args:
        text (str): String of document.

    Returns:
        document_tokens (list): List of the processed document's unigram tokens.

    """

    text_ = convert(text)

    doc = nlp(text_)
    filtered_doc = []

    for t in doc:
        # order these in rough order of likelihood
        if len(t) < 3:
            continue
        if ' ' in str(t):
            continue
        if t.is_stop:
            continue
        if t.is_digit:
            continue
        if t.pos_ == 'ADV':
            continue
        if t.pos_ == 'ADP':
            continue
        if t.is_punct:
            continue
        if t.like_url:
            continue

        filtered_doc.append(t.lower_)
    return filtered_doc
