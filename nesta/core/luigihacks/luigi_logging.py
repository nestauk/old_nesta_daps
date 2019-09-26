import logging

def set_log_level(test=False, verbose=False):
    logging.getLogger().setLevel(logging.INFO)
    if test or verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("luigi-interface").setLevel(logging.DEBUG)
    if not verbose:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("luigi-interface").setLevel(logging.WARNING)
