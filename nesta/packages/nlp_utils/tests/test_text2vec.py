import pytest
from unittest import mock

import math
import numpy as np
import pandas as pd
import tensorflow as tf
import sentencepiece as spm
import tensorflow_hub as hub


# To be tested
from nesta.packages.nlp_utils.text2vec import filter_documents
from nesta.packages.nlp_utils.text2vec import process_to_IDs_in_sparse_format
from nesta.packages.nlp_utils.text2vec import docs2vectors


def test_filter_documents():
    data = pd.DataFrame({'id': ['a', 'b', 'c', 'd', 'e'],
                         'abstractText': ['Water under the bridge.',
                                          'Who let the dogs out.',
                                          'Hello world.',
                                          np.nan,
                                          'Who let the dogs out.']})

    expected_result = pd.DataFrame({'id': ['a', 'b'],
                                    'abstractText': ['Water under the bridge.',
                                                     'Who let the dogs out.']})
    result = filter_documents(data)
    pd.testing.assert_frame_equal(result, expected_result, check_like=False)


def test_process_to_IDs_in_sparse_format():
    documents = ['Water under the bridge.', 'Who let the dogs out.']
    module = hub.Module("https://tfhub.dev/google/universal-sentence-encoder-lite/2")
    with tf.compat.v1.Session() as sess:
        spm_path = sess.run(module(signature="spm_path"))
        sp = spm.SentencePieceProcessor()
        sp.Load(spm_path)
        values, indices, dense_shape = process_to_IDs_in_sparse_format(sp, documents)
    assert(dense_shape[0] == len(documents))


@mock.patch('nesta.packages.nlp_utils.text2vec.process_to_IDs_in_sparse_format')
def test_doc2vectors(mocked_processed_documents):
    documents = ['Water under the bridge.', 'Who let the dogs out.']

    values = [4489, 315, 9, 5063, 6, 1945, 456, 9, 112, 70, 6]
    indices = [[0, 0], [0, 1], [0, 2], [0, 3], [0, 4], [1, 0],
               [1, 1], [1, 2], [1, 3], [1, 4], [1, 5]]
    dense_shape = (2, 6)

    mocked_processed_documents.return_value = values, indices, dense_shape

    doc_embeddings = docs2vectors(documents)
    assert(len(documents) == doc_embeddings.shape[0])
    assert(doc_embeddings.shape[1] == 512)
