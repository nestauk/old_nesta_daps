import numpy as np


def arr2dic(arr, thresh=None):
    """Keep the index and the values of an array if they are larger than the threshold.

    Args:
        arr (:obj:`numpy.array` of :obj:`float`): Array of numbers.
        thresh (:obj:`int`): Keep values larger than the threshold.

    Returns:
        (:obj:`dict`): Dictionary where of the format dict({arr index, arr value})

    """
    if thresh is not None:
        ids = np.where(arr >= thresh)[0]
    else:
        ids = np.arange(len(arr))

    return {id_: arr[id_] for id_ in ids}
