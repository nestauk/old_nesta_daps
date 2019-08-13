import logging


def some_func(start_string, row):
    """Do some interesting processing of the data...

    Args:
        start_string(str): the start of a name
        row(NamedTuple): a row of data

    Returns:
        (dict): processed row of data to load to the database
    """
    # do some better processing here
    data = start_string in row.name if row.name is not None else False
    return dict(my_id=row.id, data=data)


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    log_file_handler = logging.FileHandler('logs.log')
    logging.basicConfig(handlers=(log_stream_handler, log_file_handler),
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
