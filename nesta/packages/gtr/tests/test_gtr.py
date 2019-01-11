from unittest import TestCase, mock
import pytest

from nesta.packages.gtr.get_gtr_data import extract_link_table
from nesta.packages.gtr.get_gtr_data import is_list_entity
from nesta.packages.gtr.get_gtr_data import contains_key
from nesta.packages.gtr.get_gtr_data import remove_last_occurence
from nesta.packages.gtr.get_gtr_data import is_iterable
from nesta.packages.gtr.get_gtr_data import TypeDict
from nesta.packages.gtr.get_gtr_data import deduplicate_participants
from nesta.packages.gtr.get_gtr_data import unpack_funding
from nesta.packages.gtr.get_gtr_data import unpack_list_data
# from nesta.packages.gtr.get_gtr_data import extract_link_data
# from nesta.packages.gtr.get_gtr_data import extract_data
# from nesta.packages.gtr.get_gtr_data import extract_data_recursive
from nesta.packages.gtr.get_gtr_data import read_xml_from_url
from nesta.packages.gtr.get_gtr_data import get_orgs_to_geocode
from nesta.packages.gtr.get_gtr_data import geocode_uk_with_postcode
from nesta.packages.gtr.get_gtr_data import add_country_details


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


class TestGeocoding():
    @pytest.fixture
    def raw_org_data(self):
        return [(0, {'address': {'line1': 'some street', 'postCode': 'ABC 123'}}),
                (1, {'address': {'region': 'London', 'postCode': 'AA 456'}}),
                (2, {'address': {'city': 'Paris', 'region': 'Outside UK'}}),
                (3, {'address': {'id': 123, 'line1': 'my road'}}),
                (4, None)
                ]

    @pytest.fixture
    def unpacked_orgs(self):
        return [{'id': 0, 'line1': 'some street', 'postCode': 'ABC 123'},
                {'id': 1, 'region': 'London', 'postCode': 'AA 456'},
                {'id': 2, 'city': 'Paris', 'region': 'Outside UK'},
                {'id': 3, 'line1': 'my road'},
                {'id': 4}  # no address
                ]

    def test_get_orgs_to_geocode(self, raw_org_data, unpacked_orgs):
        assert get_orgs_to_geocode(raw_org_data, []) == unpacked_orgs

    def test_get_orgs_to_geocode_excludes_existing(self, raw_org_data):
        existing = [(1,), (2,), (4,)]

        expected = [{'id': 0, 'line1': 'some street', 'postCode': 'ABC 123'},
                    {'id': 3, 'line1': 'my road'}
                    ]
        assert get_orgs_to_geocode(raw_org_data, existing) == expected

    @mock.patch('nesta.packages.gtr.get_gtr_data._geocode')
    def test_geocode_correctly_calls_geocoder(self, mocked_geocode, unpacked_orgs):
        mocked_geocode.return_value = {'lat': 111, 'lon': 999}
        geocoded = geocode_uk_with_postcode(unpacked_orgs[0])

        assert mocked_geocode.mock_calls == [mock.call(postalcode='ABC 123')]
        assert geocoded == {'id': 0, 'line1': 'some street', 'postCode': 'ABC 123',
                            'latitude': 111, 'longitude': 999}

    @mock.patch('nesta.packages.gtr.get_gtr_data._geocode')
    def test_geocode_returns_empty_fields_when_address_missing(self, mocked_geocode, unpacked_orgs):
        mocked_geocode.return_value = None
        geocoded = geocode_uk_with_postcode(unpacked_orgs[4])

        mocked_geocode.assert_not_called()
        assert geocoded == {'id': 4, 'latitude': None, 'longitude': None}

    @mock.patch('nesta.packages.gtr.get_gtr_data._geocode')
    def test_geocode_returns_empty_fields_when_postcode_missing(self, mocked_geocode, unpacked_orgs):
        mocked_geocode.return_value = None
        geocoded = geocode_uk_with_postcode(unpacked_orgs[3])

        mocked_geocode.assert_not_called()
        assert geocoded == {'id': 3, 'line1': 'my road',
                            'latitude': None, 'longitude': None}

    @mock.patch('nesta.packages.gtr.get_gtr_data._geocode')
    def test_geocode_returns_empty_fields_when_outside_uk(self, mocked_geocode, unpacked_orgs):
        mocked_geocode.return_value = None
        geocoded = geocode_uk_with_postcode(unpacked_orgs[2])

        mocked_geocode.assert_not_called()
        assert geocoded == {'id': 2, 'city': 'Paris', 'region': 'Outside UK',
                            'latitude': None, 'longitude': None}

    @mock.patch('nesta.packages.gtr.get_gtr_data._geocode')
    def test_geocode_returns_empty_fields_when_geocode_fails(self, mocked_geocode, unpacked_orgs):
        mocked_geocode.return_value = None
        geocoded = geocode_uk_with_postcode(unpacked_orgs[1])

        assert mocked_geocode.mock_calls == [mock.call(postalcode='AA 456')]
        assert geocoded == {'id': 1, 'region': 'London', 'postCode': 'AA 456',
                            'latitude': None, 'longitude': None}


class TestAddCountryDetails():
    @pytest.fixture
    def org_details(self):
        return [{'id': 0, 'line1': 'some street', 'country': 'UK'},
                {'id': 1, 'region': 'London', 'country': 'United Kingdom'},
                {'id': 2, 'city': 'Paris', 'country': 'france', 'region': 'Outside UK'},
                {'id': 3, 'line1': 'my road'},  # no country
                {'id': 4, 'country': 'the moon'},  # invalid country
                {'id': 5}  # no address
                ]

    @pytest.fixture
    def continent_map(self):
        return {'FR': 'EU',
                'UK': 'EU',
                'HM': 'AN'}

    @mock.patch('nesta.packages.gtr.get_gtr_data.alpha2_to_continent_mapping')
    @mock.patch('nesta.packages.gtr.get_gtr_data.country_iso_code')
    def test_add_country_details_properly_calls_iso_coding(self, mocked_iso_code,
                                                           mocked_continent,
                                                           org_details, continent_map):
        pass

    @mock.patch('nesta.packages.gtr.get_gtr_data.alpha2_to_continent_mapping')
    @mock.patch('nesta.packages.gtr.get_gtr_data.country_iso_code')
    def test_add_country_details_correctly_applies_continent(self, mocked_iso_code,
                                                             mocked_continent, org_details):
        pass

    @mock.patch('nesta.packages.gtr.get_gtr_data.alpha2_to_continent_mapping')
    @mock.patch('nesta.packages.gtr.get_gtr_data.country_iso_code')
    def test_add_country_details_returns_empty_fields_for_failed_country_lookup(self,
                                                                                mocked_iso_code,
                                                                                mocked_continent,
                                                                                org_details):
        pass

    @mock.patch('nesta.packages.gtr.get_gtr_data.alpha2_to_continent_mapping')
    @mock.patch('nesta.packages.gtr.get_gtr_data.country_iso_code')
    def test_add_country_details_returns_empty_fields_when_no_country(self,
                                                                      mocked_iso_code,
                                                                      mocked_continent,
                                                                      org_details):
        pass

    @mock.patch('nesta.packages.gtr.get_gtr_data.alpha2_to_continent_mapping')
    @mock.patch('nesta.packages.gtr.get_gtr_data.country_iso_code')
    def test_add_country_details_returns_empty_fields_when_no_address(self,
                                                                      mocked_iso_code,
                                                                      mocked_continent,
                                                                      org_details):
        pass

    @mock.patch('nesta.packages.gtr.get_gtr_data.alpha2_to_continent_mapping')
    @mock.patch('nesta.packages.gtr.get_gtr_data.country_iso_code')
    def test_add_country_details_overwrites_country_for_uk(self, mocked_iso_code,
                                                           mocked_continent, org_details):
        pass
