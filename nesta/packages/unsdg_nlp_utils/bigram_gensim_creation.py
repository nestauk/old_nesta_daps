from gensim.models.phrases import Phrases, Phraser

def generate_bigrams(ls, bigram_model):

    return bigram_model[ls]

def rid_of_stopword_bigrams(ls, noise):

    return [i for i in ls if i not in noise]
