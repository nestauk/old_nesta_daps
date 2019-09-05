import docker
import pytest
import os
from unittest import mock

from nesta.core.luigihacks.luigi_test_runner import find_python_files
from nesta.core.luigihacks.luigi_test_runner import contains_root_task
from nesta.core.luigihacks.luigi_test_runner import find_root_tasks
from nesta.core.luigihacks.luigi_test_runner import build_docker_image
from nesta.core.luigihacks.luigi_test_runner import containerised_database
from nesta.core.luigihacks.luigi_test_runner import stop_and_remove_container

FIXTURE_DIRECTORY = 'tests/fixtures/luigi_test_runner'  # relative to luigihacks
DB_CONFIG_ENVAR = 'LUIGI_TEST_RUNNER_INTEGRATION_TESTING_DB_CONFIG'
DB_CONFIG_FILE = 'luigi_test_runner_db.config'
DB_CONTAINER_NAME = 'luigi-testing-mysql'


@pytest.fixture(scope='module')
def luigihacks_dir():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    luigihacks = os.path.join(current_dir, '../..')
    return luigihacks


@pytest.fixture(scope='module')
def fixture_dir(luigihacks_dir):
    return os.path.join(luigihacks_dir, FIXTURE_DIRECTORY)


@pytest.fixture(scope='module')
def python_files(fixture_dir):
    files = ['working_task.py',  # root task
             'subfolder/failing_task.py',  # root task
             'orphaned_task.py']  # NO root task
    return [os.path.join(fixture_dir, f) for f in files]


@pytest.fixture(scope='module')
def database_config(luigihacks_dir):
    db_config_file = os.path.join(luigihacks_dir, DB_CONFIG_FILE)
    with mock.patch.dict(os.environ, {DB_CONFIG_ENVAR: db_config_file}):
        yield


@pytest.fixture
def docker_client():
    """Creates a low level docker client."""
    return docker.APIClient()


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


class TestDockerBuild:
    def check_image_tag(self, api_client, tag):
        images = api_client.images()

        tags = {tag for tags in images
                for tag in tags['RepoTags']}

        return tag in tags

    def test_image_is_built_from_dockerfile(self, docker_client, fixture_dir):
        branch = 'test_branch'
        image_tag = 'luigi_test_runner:test'
        dockerfile = os.path.join(fixture_dir, 'Dockerfile')
        if self.check_image_tag(docker_client, image_tag):
            # clean up from previous test
            docker_client.remove_image(image_tag)

        logs = build_docker_image(image_tag=image_tag,
                                  path=fixture_dir,
                                  dockerfile=dockerfile,
                                  buildargs={'GIT_TAG': branch})
        tag_found = self.check_image_tag(docker_client, image_tag)
        logs = [l.get('stream', '').strip() for l in logs]

        assert tag_found is True
        assert f'Branch={branch}' in logs  # the branch argument is echo'd during build


class TestDatabase:
    @pytest.fixture
    def recreate_database(self, docker_client):
        try:
            docker_client.stop(DB_CONTAINER_NAME)
            docker_client.remove_container(DB_CONTAINER_NAME)
        except docker.errors.NotFound:
            # not running
            pass
        environment = {'MYSQL_ROOT_PASSWORD': 'test'}
        docker_client.create_container('mysql:5.7',
                                       name=DB_CONTAINER_NAME,
                                       environment=environment)
        docker_client.stop(DB_CONTAINER_NAME)

    def test_containerised_database_is_launched(self,
                                                recreate_database,
                                                database_config,
                                                docker_client):
        with containerised_database(name=DB_CONTAINER_NAME,
                                    db_config=DB_CONFIG_ENVAR,
                                    config_header='mysqldb'):
            containers = docker_client.containers(filters={'name': DB_CONTAINER_NAME})

            # it is not possible to run more than one container with the same name at once
            assert len(containers) == 1, "Container is not running"

    def test_luigi_updates_table_is_the_only_table(self):
        pass

    def test_database_is_removed_and_recreated_before_running(self,
                                                              recreate_database,
                                                              database_config,
                                                              docker_client):
        try:
            with containerised_database(name=DB_CONTAINER_NAME,
                                        db_config=DB_CONFIG_ENVAR,
                                        config_header='mysqldb'):
                pass
        except docker.errors.APIError:
            pytest.fail("Container was not removed before starting run")

    def test_database_is_stopped_when_exiting_context_manager(self,
                                                              recreate_database,
                                                              database_config,
                                                              docker_client):
        with containerised_database(name=DB_CONTAINER_NAME,
                                    db_config=DB_CONFIG_ENVAR,
                                    config_header='mysqldb'):
            pass

        containers = docker_client.containers(filters={'name': DB_CONTAINER_NAME})
        assert len(containers) == 0, "Container is still running"

    def test_container_is_stopped_and_removed(self,
                                              recreate_database,
                                              docker_client):
        stop_and_remove_container(DB_CONTAINER_NAME)

        containers = docker_client.containers(filters={'name': DB_CONTAINER_NAME},
                                              all=True)

        assert len(containers) == 0, "Container still exists"

    def test_stop_and_remove_works_on_already_stopped_containers(self,
                                                                 recreate_database,
                                                                 docker_client):
        stop_and_remove_container(DB_CONTAINER_NAME)

        try:
            stop_and_remove_container(DB_CONTAINER_NAME)
        except Exception:
            pytest.fail("Failed to run stop and remove on stopped container")
