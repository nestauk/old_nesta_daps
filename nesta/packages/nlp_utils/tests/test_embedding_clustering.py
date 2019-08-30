import pytest
import numpy as np
from sklearn.datasets import load_iris
from nesta.packages.nlp_utils.embed_clustering import clustering


def test_gaussian_mixture_clustering():
    arr = np.random.rand(1000, 3)
    # Test reproducibility
    gmm_model_v1 = clustering(arr)
    gmm_model_v2 = clustering(arr)
    np.testing.assert_array_equal(
        gmm_model_v1.predict_proba(arr), gmm_model_v2.predict_proba(arr)
    )

    iris = load_iris()["data"]
    gmm_model_v3 = clustering(iris, n_components=4)
    arr1 = gmm_model_v3.predict(iris)
    scores = []
    for i in range(1, 11):
        gmm_model_v4 = clustering(iris, n_components=4, random_state=i)
        arr2 = gmm_model_v4.predict(iris)
        scores.append(
            (
                len(
                    set(np.where(arr1 == arr1[0])[0]).intersection(
                        np.where(arr2 == arr2[0])[0]
                    )
                )
                / len(set(np.where(arr1 == arr1[0])[0]))
            )
        )
    # np.testing.assert_array_equal(arr1, arr2)
    assert np.mean(scores) > 0.95
