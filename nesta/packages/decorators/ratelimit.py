'''
ratelimit
=========

Apply rate limiting at a threshold per second
'''

import time

def ratelimit(max_per_second):
    '''
    Args:
        max_per_second (float): Number of permitted hits per second
    '''
    min_interval = 1.0 / float(max_per_second)
    def decorate(func):
        last_time_called = [time.clock()]  # `list`s are globally persistified
        def rate_limited(*args, **kargs):
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args,**kargs)
            last_time_called[0] = time.clock()
            return ret
        return rate_limited  # Note: returning a method
    return decorate  # Note: returning a method
