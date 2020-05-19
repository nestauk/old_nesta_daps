from dateutil import rrule
from datetime import datetime as dt
from datetime import timedelta
from pandas import Timestamp
from toolz.itertoolz import sliding_window


def weekchunks(start, until=None, until_days_ago=0, 
               date_format='%Y-%m-%d'):
    '''Generate date strings in weekly chunks between two dates.
    Args:
        start (str): Sensibly formatted datestring (format to be guessed by pd)
        until (str): Another datestring. Default=today.
        until_days_ago (str): if until is not specified, this indicates how many days ago to consider. Default=0.
                              if until is specified, this indicates an offset of days before the until date.
        date_format (str): Date format of the output date strings.
    Returns:
        chunk_pairs (list): List of pairs of string, representing the start and end of weeks.
    '''
    until = Timestamp(until).to_pydatetime() if until is not None else dt.now() - timedelta(days=until_days_ago)
    start = Timestamp(start).to_pydatetime()
    chunks = [dt.strftime(_date, date_format)
              for _date in rrule.rrule(rrule.WEEKLY, dtstart=start,
                                       until=until)]
    if len(chunks) == 1:  # the less-than-one-week case
        _until = dt.strftime(until, date_format)
        chunks.append(_until)
    chunk_pairs = list(sliding_window(2, chunks))
    return chunk_pairs
