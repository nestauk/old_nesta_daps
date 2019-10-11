import pytest
import csv
from nesta.packages.reviews import esco_loader

# @pytest.fixture
# def mock_csv_file():
#     column_names = ['col_1', 'col_2']
#     dict_data = [
#         {'col_1': 1, 'col_2': 2}
#         {'col_1': 3, 'col_2': 4}
#     csv_file ="mock_data.csv"
#     with open(csv_file, 'w') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames = column_names)
#             writer.writeheader()
#             for row in dict_data:
#                     writer.writerow(row)
#     return csv_file

# class TestLoadDataFromBucket:
#     def test_loading_csv(self):
#             data = esco_loader.load_csv_as_dict('https://karlis-random-stuff.s3.eu-west-2.amazonaws.com/occupations_skills_link_limited.csv')
#             print(data)
#             assert 1==1, 'test failed'
