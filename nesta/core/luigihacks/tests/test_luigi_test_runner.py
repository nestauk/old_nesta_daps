import pytest
from unittest import mock

from nesta.core.luigihacks.luigi_test_runner import luigi_test_runner
from nesta.core.luigihacks.luigi_test_runner import find_root_tasks
from nesta.core.luigihacks.luigi_test_runner import find_python_files


@mock.patch('nesta.core.luigihacks.luigi_test_runner.run_luigi_pipeline', autospec=True)
@mock.patch('nesta.core.luigihacks.luigi_test_runner.recreate_test_database', autospec=True)
@mock.patch('nesta.core.luigihacks.luigi_test_runner.build_docker_image', autospec=True)
@mock.patch('nesta.core.luigihacks.luigi_test_runner.find_root_tasks', autospec=True)
class TestTestRunner:
    @pytest.fixture
    def modules_to_find(self):
        return ['routines.mag.mag_root_task',
                'routines.crunchbase.crunchbase_root_task',
                'routines.nih.nih_root_task']

    def test_find_root_tasks_is_called_correctly(self,
                                                 mocked_find_root_tasks,
                                                 mocked_build_image,
                                                 mocked_recreate_database,
                                                 mocked_run_pipeline):
        luigi_test_runner(start_directory='routines')

        mocked_find_root_tasks.assert_called_once_with(start_directory='routines')

    def test_build_docker_image_is_called_with_specified_branch(self,
                                                                mocked_find_root_tasks,
                                                                mocked_build_image,
                                                                mocked_recreate_database,
                                                                mocked_run_pipeline):
        luigi_test_runner(start_directory='routines', branch='new_feature')

        mocked_build_image.assert_called_once_with(branch='new_feature')

    def test_recreate_database_is_called_correctly(self,
                                                   mocked_find_root_tasks,
                                                   mocked_build_image,
                                                   mocked_recreate_database,
                                                   mocked_run_pipeline,
                                                   modules_to_find):
        mocked_find_root_tasks.return_value = modules_to_find
        luigi_test_runner(start_directory='routines')

        assert mocked_recreate_database.call_count == len(modules_to_find)

    def test_all_pipelines_are_run(self,
                                   mocked_find_root_tasks,
                                   mocked_build_image,
                                   mocked_recreate_database,
                                   mocked_run_pipeline,
                                   modules_to_find):
        mocked_find_root_tasks.return_value = modules_to_find
        expected_result = [mock.call(module) for module in modules_to_find]

        luigi_test_runner(start_directory='routines')

        assert mocked_run_pipeline.mock_calls == expected_result

    def test_all_pipelines_are_still_run_when_they_error(self,
                                                         mocked_find_root_tasks,
                                                         mocked_build_image,
                                                         mocked_recreate_database,
                                                         mocked_run_pipeline,
                                                         modules_to_find):
        mocked_find_root_tasks.return_value = modules_to_find
        mocked_run_pipeline.side_effect = [ValueError, ZeroDivisionError, None]

        luigi_test_runner(start_directory='routines')

        assert mocked_run_pipeline.call_count == 3


@mock.patch('nesta.core.luigihacks.luigi_test_runner.contains_root_task')
@mock.patch('nesta.core.luigihacks.luigi_test_runner.find_python_files')
class TestFindRootTasks:
    @pytest.fixture
    def paths_to_find(self):
        return ['routines/mag/mag_root_task.py',
                'routines/crunchbase/crunchbase_root_task.py',
                'routines/nih/nih_root_task.py']

    def test_all_found_tasks_are_returned_in_module_format(self,
                                                           mocked_find_python,
                                                           mocked_contains_root,
                                                           paths_to_find):
        mocked_find_python.return_value = paths_to_find
        mocked_contains_root.return_value = True
        expected_result = ['routines.mag.mag_root_task',
                           'routines.crunchbase.crunchbase_root_task',
                           'routines.nih.nih_root_task']

        result = find_root_tasks(start_directory='routines')

        assert result == expected_result

    def test_files_not_contining_root_tasks_are_excluded(self,
                                                         mocked_find_python,
                                                         mocked_contains_root,
                                                         paths_to_find):
        mocked_find_python.return_value = paths_to_find
        mocked_contains_root.side_effect = [True, False, True]

        result = find_root_tasks(start_directory='routines')

        assert len(result) == 2
        assert 'routines.crunchbase.crunchbase_root_task' not in result


@mock.patch('nesta.core.luigihacks.luigi_test_runner.glob.glob')
class TestFindPythonFiles:
    def test_glob_is_called_correctly(self, mocked_glob):
        find_python_files('subfolder/routines')

        mocked_glob.assert_called_once_with('subfolder/routines/**/*.py', recursive=True)

    def test_trailing_slash_is_removed_from_start_directory(self, mocked_glob):
        find_python_files('subfolder/routines/')

        assert mocked_glob.call_args[0][0] == 'subfolder/routines/**/*.py'
