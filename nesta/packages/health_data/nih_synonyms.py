# from nesta.packages.health_data.nih_abstract_yielder import AbstractYielder
# from nesta.packages.nlp_utils.preprocess import tokenize_document
# from nesta.packages.nlp_utils.preprocess import build_ngrams
# from nesta.packages.nlp_utils.preprocess import filter_by_idf
# from gensim.models import Word2Vec
# from jklearn.cluster.omnislash import Omnislash
# import numpy as np
# import pandas as pd


# def process_abstracts(chunksize, ngram_size, tfidf_low, tfidf_high):
#     with AbstractYielder() as ay:
#         docs = []
#         for _, abstract_text in ay.iterrows(chunksize=chunksize):
#             if abstract_text is None:
#                 continue
#             docs.append(tokenize_document(abstract_text))

#     docs = build_ngrams(docs, n=ngram_size)
#     docs = [d for d in docs if len(d) > 0]
#     docs = filter_by_idf(docs, tfidf_low, tfidf_high)
#     _docs = []
#     for d in docs:
#         _docs += d
#     return _docs


# if __name__ == '__main__':
#     print("getting abstracts")
#     docs = process_abstracts(chunksize=1000,
#                              ngram_size=2,
#                              tfidf_low=10,
#                              tfidf_high=99)

#     print("making w2v")
#     model = Word2Vec(docs, size=300, window=5,
#                      min_count=5, workers=4, iter=2000)
#     model.wv.init_sims(replace=True)
#     model.save("nih_wv.bin")

#     condition = pd.isnull(model.wv.vectors).sum(axis=1).astype(bool)
#     print("dropping", (condition).sum())
#     print("keeping", (~condition).sum())
#     vectors = model.wv.vectors[~condition]
#     words = np.array(model.wv.index2word)[~condition]
#     del model

#     print("training omnislash")
#     omni = Omnislash(1000, evr_max=0.75, svd_solver='arpack',
#                      n_components_max=10)
#     labels = omni.fit_predict(vectors)
#     print("found", len(set(labels)), "groups")

#     from collections import Counter
#     print(Counter(labels))
