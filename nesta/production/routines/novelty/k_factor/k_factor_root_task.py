'''
K factor calculation and processing
===================================

Luigi routine to apply the K Factor to documents in ES that have lists of 
terms.
'''

import luigi
import datetime
import logging

from nesta.production.luigihacks.misctools import find_filepath_from_path_stup as f3p

class RootTask(luigi.WrapperTask):

    def dummy(self):
        pass
