import pytest
import mock
import pandas as pd
from nesta.packages.decorators.schema_transform import schema_transform
from nesta.packages.decorators.schema_transform import schema_transformer


class TestSchemaTransform():

    @staticmethod
    @pytest.fixture
    def test_data():
        return [{"bad_col": 1, "another_bad_col": 2, "one_more_bad_col": -1}]

    @staticmethod
    @pytest.fixture
    def test_transformer():
        return {"bad_col": "good_col",
                "another_bad_col": "another_good_col"}

    @mock.patch('nesta.packages.decorators.schema_transform.load_transformer')
    def test_dataframe_transform(self, mocked_loader, test_transformer, test_data):
        mocked_loader.return_value = test_transformer
        dummy_func = lambda : pd.DataFrame(test_data)
        wrapper = schema_transform("dummy", "dummy", "dummy")
        wrapped = wrapper(dummy_func)
        transformed = wrapped()

        assert len(transformed.columns) == len(test_transformer)
        assert all(c in test_transformer.values()
                   for c in transformed.columns)

    @mock.patch('nesta.packages.decorators.schema_transform.load_transformer')
    def test_list_of_dict_transform(self, mocked_loader, test_transformer, test_data):
        mocked_loader.return_value = test_transformer
        dummy_func = lambda : test_data
        wrapper = schema_transform("dummy", "dummy", "dummy")
        wrapped = wrapper(dummy_func)
        transformed = wrapped()
        transformed = pd.DataFrame(transformed)
        assert len(transformed.columns) == len(test_transformer)
        assert all(c in test_transformer.values()
                   for c in transformed.columns)

    @mock.patch('nesta.packages.decorators.schema_transform.load_transformer')
    def test_invalid_type_transform(self, mocked_loader, test_transformer):
        mocked_loader.return_value = test_transformer
        dummy_func = lambda : None
        wrapper = schema_transform("dummy", "dummy", "dummy")
        wrapped = wrapper(dummy_func)
        with pytest.raises(ValueError) as e:
            wrapped()
        assert "Schema transform expects" in str(e.value)

    @mock.patch('nesta.packages.decorators.schema_transform.load_transformer')
    def test_single_dict(self, mocked_loader, test_transformer):
        mocked_loader.return_value = test_transformer
        test_data = {'bad_col': 111, 'another_bad_col': 222, 'stuff': 333}

        transformed = schema_transformer(test_data, filename='dummy',
                                         from_key='dummy', to_key='dummy')
        assert transformed == {'good_col': 111, 'another_good_col': 222}
