import unittest

from country_extended_groups import CountryGroupsTask
from country_extended_groups import GroupsMembersTask
from country_extended_groups import MembersGroupsTask

class PrepareTest(unittest.TestCase):
    dummy_kwargs = dict(iso2="MX", category="34",
                        env_files=[""], _routine_id="", batchable="", 
                        job_def="", job_name="", 
                        job_queue="", region_name="")
    
    def test_country_groups(self):
        task = CountryGroupsTask(**self.dummy_kwargs)
        task.prepare()


    def test_groups_members(self):
        task = GroupsMembersTask(**self.dummy_kwargs)
        task.prepare()


    def test_members_groups(self):
        task = MembersGroupsTask(**self.dummy_kwargs)
        task.prepare()


class OutputTest(unittest.TestCase):
    dummy_kwargs = dict(iso2="MX", category="34",
                        env_files=[""], _routine_id="", batchable="", 
                        job_def="", job_name="", 
                        job_queue="", region_name="")
    
    def test_country_groups(self):
        task = CountryGroupsTask(**self.dummy_kwargs)
        task.output()


    def test_groups_members(self):
        task = GroupsMembersTask(**self.dummy_kwargs)
        task.output()


    def test_members_groups(self):
        task = MembersGroupsTask(**self.dummy_kwargs)
        task.output()



if __name__ == "__main__":
    unittest.main()
