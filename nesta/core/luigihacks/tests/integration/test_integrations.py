import pytest
import os

from nesta.core.luigihacks.luigi_test_runner import find_python_files
from nesta.core.luigihacks.luigi_test_runner import contains_root_task
from nesta.core.luigihacks.luigi_test_runner import find_root_tasks

FIXTURE_DIRECTORY = 'fixtures/luigi_test_runner'  # relative to the tests/ directory


@pytest.fixture(scope='module')
def fixture_dir():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    one_dir_up = os.path.split(current_dir)[0]

    return os.path.join(one_dir_up, FIXTURE_DIRECTORY)


@pytest.fixture(scope='module')
def python_files(fixture_dir):
    files = ['working_task.py',  # root task
             'subfolder/failing_task.py',  # root task
             'orphaned_task.py']  # NO root task

    return [os.path.join(fixture_dir, f) for f in files]


class TestFindRootTasks:
    def test_all_python_files_are_found(self, fixture_dir, python_files):
        result = find_python_files(fixture_dir)

        assert sorted(result) == sorted(python_files)

    def test_root_tasks_are_correctly_identified(self, python_files):
        expected_result = [True, True, False]

        result = [contains_root_task(f) for f in python_files]

        assert result == expected_result

    def test_all_python_files_are_found_and_converted_to_modules(self,
                                                                 fixture_dir,
                                                                 python_files):
        expected_result = [f.replace('/', '.')[:-3]
                           for f in python_files[0:2]]

        result = find_root_tasks(fixture_dir)

        assert sorted(result) == sorted(expected_result)
