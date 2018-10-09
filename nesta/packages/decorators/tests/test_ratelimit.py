import pytest
import time

from nesta.packages.decorators.ratelimit import ratelimit

class TestRateLimit():
    def test_rate_limit(self):
        dummy_func = lambda : None
        wrapper = ratelimit(1)
        wrapped = wrapper(dummy_func)
        previous_time = time.time()
        for i in range(0, 3):
            wrapped()
            assert time.time() - previous_time > 1
            previous_time = time.time()

