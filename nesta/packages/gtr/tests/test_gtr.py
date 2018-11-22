from unittest import TestCase

from nesta.packages.gtr.get_gtr_data import extract_link_table
from nesta.packages.gtr.get_gtr_data import is_list_entity
from nesta.packages.gtr.get_gtr_data import contains_key
from nesta.packages.gtr.get_gtr_data import remove_last_occurence
from nesta.packages.gtr.get_gtr_data import is_iterable
from nesta.packages.gtr.get_gtr_data import TypeDict
from nesta.packages.gtr.get_gtr_data import deduplicate_participants
from nesta.packages.gtr.get_gtr_data import unpack_funding
from nesta.packages.gtr.get_gtr_data import unpack_list_data
from nesta.packages.gtr.get_gtr_data import extract_link_data
from nesta.packages.gtr.get_gtr_data import extract_data
from nesta.packages.gtr.get_gtr_data import extract_data_recursive
from nesta.packages.gtr.get_gtr_data import read_xml_from_url

class TestGtr(TestCase):
    def test_extract_link_table(self):
        data = {"example_table_1":[{"project_id": 1, "rel": 2, "id":1}, 
                                   {"project_id": 1, "other": 3},
                                   {"rel": 1, "other": 3}],
                "example_table_2":[{"project_id": 1, "rel": 2, "id":1},
                                   {"project_id": 1, "rel": 2, "id":1},
                                   {"rel": 1, "other": 3}]}    
        extract_link_table(data)
        self.assertIn("link_table", data)
        self.assertEqual(len(data["link_table"]), 3)

    def test_is_list_entity(self):
        entity_pass_1 = {"key": [{"key_2":"value"}]}
        entity_pass_2 = {"key": []}
        self.assertTrue(is_list_entity(entity_pass_1))
        self.assertTrue(is_list_entity(entity_pass_2))

        entity_fail_1 = [{"key": [{"key_2":"value"}]}]
        entity_fail_2 = {"key": "value"}
        self.assertFalse(is_list_entity(entity_fail_1))
        self.assertFalse(is_list_entity(entity_fail_2))
        self.assertFalse(is_list_entity([]))
        self.assertFalse(is_list_entity(None))
        self.assertFalse(is_list_entity(2))

    def test_contains_key(self):
        data = {"example_table_1":[{"project_id": 1, "rel": 2},
                                   {"project_id": 1, "other": 3},
                                   {"rel": 1, "other": 3}],
                "example_table_2":[{"project_id": 1, "rel": 2},
                                   {"project_id": 1, "rel": 2},
                                   {"rel": 1, "other": 3}]}

        for pass_key in ("project_id", "rel", "other", 
                         "example_table_1", "example_table_2"):
            self.assertTrue(contains_key(data, pass_key))

        for fail_key in ("something_else", "another"):
            self.assertFalse(contains_key(data, fail_key))

    def test_remove_last_occurence(self):
        result = remove_last_occurence("some:other:object", ":")
        self.assertEqual(result, "some:otherobject")
        
    def test_is_iterable(self):
        for pass_iter in ("text", {}, tuple(), [], set()):
            self.assertTrue(is_iterable(pass_iter))

        for fail_iter in (1, 1.):
            self.assertFalse(is_iterable(fail_iter))

    def test_TypeDict(self):
        data = TypeDict()
        self.assertTrue(isinstance(data, dict))
        
        data['nil_value'] = {'nil': 'true'}
        data['greeting'] = "hello"
        data['signoff'] = "good bye"
        data['already_int'] = 1
        data['one'] = "1"
        data['one_point'] = "1."

        self.assertEqual(data['nil_value'], None)
        self.assertEqual(data['greeting'], "hello")
        self.assertEqual(data['signoff'], "good bye")
        self.assertEqual(data['already_int'], 1)
        self.assertEqual(data['one'], 1)
        self.assertEqual(data['one_point'], 1.)

        
    def test_deduplicate_participants(self):
        data = {'participant': [{"organisationId":20, 'projectCost':1, 'grantOffer':0}],
                'organisations': [{'id':20}]}
        deduplicate_participants(data)
        self.assertNotIn('participant', data)
        self.assertIn('projectCost', data['organisations'][0])
        self.assertIn('grantOffer', data['organisations'][0])
        
    def test_unpack_funding(self):
        row = {"money_stuff":{"currencyCode":"GBP", "value":20},
               "other_stuff":"a_value"}
        unpack_funding(row)
        self.assertNotIn("money_stuff", row)
        self.assertIn("currencyCode", row)
        self.assertIn("value", row)
        self.assertIn("other_stuff", row)        

    def test_unpack_list_data(self):
        from collections import defaultdict
        data = defaultdict(list)
        row = {"id":1,
               "links": {"link": [{"entity":"projects", "key":"value", "id":1}]},
               "generic_items": {"generic_item": [{"some_data":100, "id":1}]},
               "researchTopics": {"researchTopic": [{"percentage":100, "text":"value", "id":1}]}}
        unpack_list_data(row, data)
        self.assertIn('projects', data)
        self.assertIn('generic_item', data)
        self.assertIn('topic', data)        
        self.assertNotIn('percentage', data['topic'][0])

    def test_read_xml_from_url(self):
        read_xml_from_url("https://gtr.ukri.org/gtr/api/projects")
