from numpy.testing import assert_almost_equal
import pytest
from unittest import mock

from nesta.packages.grid.grid import ComboFuzzer
from nesta.packages.grid.grid import grid_name_lookup
from nesta.production.orms.grid_orm import Institute, Alias


class TestComboFuzzer:
    def test_combo_fuzzer_sets_normalisation_variable(self):
        fuzzer = ComboFuzzer(fuzzers=['mock_fuzzer1', 'mock_fuzzer2'])
        assert_almost_equal(fuzzer.norm, 0.70710, decimal=5)

        fuzzer = ComboFuzzer(fuzzers=['mock_fuzzer1'])
        assert fuzzer.norm == 1

    def test_combo_fuzz_scorer_returns_consistent_results(self):
        mock_fuzzer1 = mock.Mock(side_effect=[90, 100, 60, 0])
        mock_fuzzer2 = mock.Mock(side_effect=[95, 100, 10, 0])
        fuzzer = ComboFuzzer(fuzzers=[mock_fuzzer1, mock_fuzzer2])

        for expected_score in [0.92533, 1, 0.43011, 0]:
            assert_almost_equal(fuzzer.combo_fuzz('a', 'b'), expected_score, decimal=5)

    @mock.patch("nesta.packages.grid.grid.fuzzy_proc.extractOne")
    def test_fuzzy_match_doesnt_keep_history_by_default(self, mocked_fuzzy_extract):
        mocked_fuzzy_extract.return_value = ('c', 0.9)

        fuzzer = ComboFuzzer(fuzzers=['mock_fuzzer1', 'mock_fuzzer2'])
        fuzzer.fuzzy_match_one('a', ['a', 'b'])
        fuzzer.fuzzy_match_one('a', ['a', 'b'])

        # not storing history so fuzzy match is done twice
        assert mocked_fuzzy_extract.call_count == 2

    @mock.patch("nesta.packages.grid.grid.fuzzy_proc.extractOne")
    def test_fuzzy_match_one_checks_previous_successful(self, mocked_fuzzy_extract):
        mocked_fuzzy_extract.return_value = ('c', 0.9)

        fuzzer = ComboFuzzer(fuzzers=['mock_fuzzer1', 'mock_fuzzer2'], store_history=True)
        fuzzer.fuzzy_match_one('a', ['a', 'b'])
        fuzzer.fuzzy_match_one('a', ['a', 'b'])

        # storing history so fuzzy match is done once only
        assert mocked_fuzzy_extract.call_count == 1

    @mock.patch("nesta.packages.grid.grid.fuzzy_proc.extractOne")
    def test_fuzzy_match_one_raises_key_error_when_previously_failed(self,
                                                                     mocked_fuzzy_extract):
        mocked_fuzzy_extract.return_value = ('c', 0.5)

        fuzzer = ComboFuzzer(fuzzers=['mock_fuzzer1', 'mock_fuzzer2'], store_history=True)
        try:
            # should be added to failed history here
            fuzzer.fuzzy_match_one('a', ['a', 'b'], lowest_match_score=0.6)
        except KeyError:
            pass

        with pytest.raises(KeyError):
            fuzzer.fuzzy_match_one('a', ['a', 'b'], lowest_match_score=0.6)
        # storing history so fuzzy match is done once only
        assert mocked_fuzzy_extract.call_count == 1

    @mock.patch("nesta.packages.grid.grid.fuzzy_proc.extractOne")
    def test_fuzzy_match_one_raises_key_error_when_score_below_lowest_match_score(self,
                                                                                  mocked_fuzzy_extract):
        mocked_fuzzy_extract.return_value = ('c', 0.5)

        fuzzer = ComboFuzzer(fuzzers=['mock_fuzzer1', 'mock_fuzzer2'], store_history=True)

        with pytest.raises(KeyError):
            fuzzer.fuzzy_match_one('a', ['a', 'b'], lowest_match_score=0.6)


class TestGridNameLookup:
    @pytest.fixture
    def mocked_session(self):
        mocked_session = mock.Mock()
        all_institutes = [Institute(id='1', name='TEST Institute'),
                          Institute(id='2', name='Institute of Testing')]

        all_aliases = [Alias(grid_id='1', alias='Some Other Name'),
                       Alias(grid_id='3', alias='Additional Alias')]

        all_with_country_name = [Institute(id='4', name='Country Institute (France)'),
                                 Institute(id='5', name='Test Place (Germany)')]

        mocked_session.query().all.side_effect = [all_institutes, all_aliases]
        mocked_session.query().filter().all.return_value = all_with_country_name

        return mocked_session

    @mock.patch("nesta.packages.grid.grid.db_session", autospec=True)
    def test_institute_names_are_all_lowered(self, mocked_db_session, mocked_session):
        mocked_engine = mock.Mock()
        mocked_db_session(mocked_engine).__enter__.return_value = mocked_session

        lookup = grid_name_lookup(mocked_engine)
        for name, _ in lookup.items():
            assert name == name.lower()

    @mock.patch("nesta.packages.grid.grid.db_session", autospec=True)
    def test_institute_names_and_ids_are_found_in_final_dataset(self,
                                                                mocked_db_session,
                                                                mocked_session):
        mocked_engine = mock.Mock()
        mocked_db_session(mocked_engine).__enter__.return_value = mocked_session

        lookup = grid_name_lookup(mocked_engine)
        assert lookup['test institute'] == ['1']
        assert lookup['institute of testing'] == ['2']

    @mock.patch("nesta.packages.grid.grid.db_session", autospec=True)
    def test_aliases_and_ids_are_found_in_final_dataset(self,
                                                        mocked_db_session,
                                                        mocked_session):
        mocked_engine = mock.Mock()
        mocked_db_session(mocked_engine).__enter__.return_value = mocked_session

        lookup = grid_name_lookup(mocked_engine)
        assert lookup['some other name'] == ['1']
        assert lookup['additional alias'] == ['3']

    @mock.patch("nesta.packages.grid.grid.db_session", autospec=True)
    def test_names_with_brackets_are_found_with_have_country_removed(self,
                                                                     mocked_db_session,
                                                                     mocked_session):
        mocked_engine = mock.Mock()
        mocked_db_session(mocked_engine).__enter__.return_value = mocked_session

        lookup = grid_name_lookup(mocked_engine)
        assert lookup['country institute'] == ['4']
        assert lookup['test place'] == ['5']
