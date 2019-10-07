import numpy as np
from sklearn.mixture import GaussianMixture


def clustering(
    vectors,
    covariance_type="tied",
    init_params="kmeans",
    max_iter=100,
    means_init=None,
    n_components=80,
    n_init=1,
    precisions_init=None,
    random_state=42,
    reg_covar=1e-06,
    tol=0.001,
    verbose=0,
    verbose_interval=10,
    warm_start=False,
    weights_init=None,
):
    """Vector clustering with Gaussian Mixtures.

    Args:
        vectors (:obj:`numpy.array` of :obj:`float`): Document vectors.

    Return:
        gmm (:obj:`sklearn.mixture.gaussian_mixture.GaussianMixture`): Fitted Gaussian Mixture model.

    """
    # For now, GMMs hyperparameters are hardcoded.
    gmm = GaussianMixture(
        covariance_type=covariance_type,
        init_params=init_params,
        max_iter=max_iter,
        means_init=means_init,
        n_components=n_components,
        n_init=n_init,
        precisions_init=precisions_init,
        random_state=random_state,
        reg_covar=reg_covar,
        tol=tol,
        verbose=verbose,
        verbose_interval=verbose_interval,
        warm_start=warm_start,
        weights_init=weights_init,
    )
    gmm.fit(vectors)
    return gmm
