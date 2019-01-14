"""Utilties for working with batches."""


def split_batches(data, batch_size):
    """Breaks batches down into chunks consumable by the database.

    Args:
        data (:obj:`list` of :obj:`dict`): list of rows of data
        batch_size (int): number of rows per batch

    Returns:
        (:obj:`list` of :obj:`dict`): yields a batch at a time
    """
    batch = []
    for row in data:
        batch.append(row)
        if len(batch) == batch_size:
            yield batch
            batch.clear()
    if len(batch) > 0:
        yield batch


