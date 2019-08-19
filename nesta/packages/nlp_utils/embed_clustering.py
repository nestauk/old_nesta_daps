import numpy as np
from sklearn.mixture import GaussianMixture

np.random.seed(42)


def vals2arr(d):
    """Store dictionary keys and values in arrays."""
    return list(d.keys()), np.array(list(d.values()))


def clustering(vectors):
    """Vector clustering with Gaussian Mixtures.

    Args:
        vectors (:obj:`numpy.array` of :obj:`float`): Document vectors.

    Return:
        gmm (:obj:`sklearn.mixture.gaussian_mixture.GaussianMixture`): Fitted Gaussian Mixture model.

    """
    # For now, GMMs hyperparameters are hardcoded.
    gmm = GaussianMixture(covariance_type='tied', init_params='kmeans', max_iter=100,
                          means_init=None, n_components=80, n_init=1, precisions_init=None,
                          random_state=42, reg_covar=1e-06, tol=0.001, verbose=0,
                          verbose_interval=10, warm_start=False, weights_init=None)
    gmm.fit(vectors)
    return gmm


def filter_arr(arr, thresh=.2):
    """Keep the index and the values of an array if they are larger than the threshold.

    Args:
        arr (:obj:`numpy.array` of :obj:`float`): Array of numbers.
        thresh (:obj:`int`): Keep values larger than the threshold.

    Returns:
        (:obj:`dict`): Dictionary where of the format dict({arr index, probability})

    """
    ids = np.where(arr >= thresh)[0]
    return {id_: arr[id_] for id_ in ids}
