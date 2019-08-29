import sys
import time
import numpy as np
import pandas as pd
import tensorflow as tf
import sentencepiece as spm
import tensorflow_hub as hub

np.random.seed(42)
USE_LITE = "https://tfhub.dev/google/universal-sentence-encoder-lite/2"


def process_to_IDs_in_sparse_format(sp, documents):
    """Process documents with the SentencePiece processor. The results have a format
    similar to tf.SparseTensor (values, indices, dense_shape)."""
    ids = [sp.EncodeAsIds(x) for x in documents]
    max_len = max(len(x) for x in ids)
    dense_shape = (len(ids), max_len)
    values = [item for sublist in ids for item in sublist]
    indices = [[row, col] for row in range(len(ids)) for col in range(len(ids[row]))]
    return (values, indices, dense_shape)


def docs2vectors(documents):
    """Find the vector representation of a collection of documents using Google's
    Universal Sentence Encoder (lite) model.

    Args:
        documents (:obj:`list` of :obj:`str`): List of raw text documents.

    Returns:
        doc_embeddings (:obj:`numpy.array` of :obj:`numpy.array` of :obj:`float`): Vector representation of documents.

    """
    module = hub.Module(USE_LITE)
    input_placeholder = tf.compat.v1.sparse_placeholder(tf.int64, shape=[None, None])
    encodings = module(
        inputs=dict(
            values=input_placeholder.values,
            indices=input_placeholder.indices,
            dense_shape=input_placeholder.dense_shape))

    with tf.compat.v1.Session() as sess:
        spm_path = sess.run(module(signature="spm_path"))
        sp = spm.SentencePieceProcessor()
        sp.Load(spm_path)
        # Preprocess documents
        values, indices, dense_shape = process_to_IDs_in_sparse_format(sp, documents)
        sess.run([tf.compat.v1.global_variables_initializer(), tf.compat.v1.tables_initializer()])
        doc_embeddings = sess.run(
                                encodings,
                                feed_dict={input_placeholder.values: values,
                                           input_placeholder.indices: indices,
                                           input_placeholder.dense_shape: dense_shape})
    return doc_embeddings
