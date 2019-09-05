from contextlib import contextmanager
import docker
import glob
import logging
import os
import sqlalchemy
import re
import time

from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.luigihacks import misctools


def find_python_files(start_directory):
    """Locates all .py files recursively from a supplied start path.

    Args:
        start_directory(str): path to start searching from

    Returns:
        (list): all found files
    """
    if start_directory.endswith('/'):
        start_directory = start_directory[:-1]

    path = f"{start_directory}/**/*.py"
    python_files = glob.glob(path, recursive=True)

    return python_files


def contains_root_task(file_path):
    """Searches line by line through a file looking for a RootTask class.

    Args:
        file_path(str): path to the file to check

    Returns:
        (bool): True if the file contains a RootTask
    """
    root_task_class = 'class RootTask(luigi.WrapperTask):\n'

    for line in open(file_path, mode='r'):
        if line == root_task_class:
            return True

    return False


def find_root_tasks(start_directory):
    """Locates python files containing root tasks and returns them in module format so
    they can be run from the Luigi cli.

    Args:
        start_directory(str): path to the folder containing routines

    Returns:
        (list): module style paths for each file containing a RootTask
    """
    def repl(match):
        """Forward slashes are replaced with '.' everything else is removed, ie '.py'"""
        if match.group(0) == '/':
            return '.'

    rex = re.compile(r'\/|\.py')
    modules = []
    for python_file in find_python_files(start_directory):
        if not contains_root_task(python_file):
            continue

        # convert to module_format
        reformatted = re.sub(rex, repl, python_file)
        modules.append(reformatted)

    return modules


def build_docker_image(image_tag,
                       path='.',
                       dockerfile='docker/Dockerfile',
                       buildargs=None):
    """Builds the specified image from a Dockerfile.

    Args:
        image_tag(str): name and tag for the build image in the format name:tag
        path(str): context for the build
        dockerfile(str): path to the Dockefile
        buildargs(dict): arguments to supply to the build

    Yields:
        (dict): build logs
    """
    client = docker.from_env()

    _, logs = client.images.build(path=path, dockerfile=dockerfile, tag=image_tag,
                                  nocache=True, rm=True, buildargs=buildargs)

    return logs


def stop_and_remove_container(name):
    """Stops and removes a container with the supplied name.

    Args:
        name(str): name of the container
    """
    client = docker.from_env()
    # there can only be one container with the same name
    try:
        container = client.containers.list(filters={'name': name}, all=True)[0]
    except IndexError:
        # not found
        pass
    else:
        container.stop()
        container.remove()


def create_luigi_table_updates(db_config, config_header, database):
    """Creates the table luigi_table_updates in the specified database.

    Args:
        db_config(str): environmental variable containing the path to the .config file
        config_header(str): header in the config file
        database(str): name of the database
    """
    engine = get_mysql_engine(db_config, config_header, database)
    connection = engine.connect()
    try:
        connection.execute("CREATE TABLE luigi_table_updates ("
                           "id            BIGINT(20)    NOT NULL AUTO_INCREMENT,"
                           "update_id     VARCHAR(128)  NOT NULL,"
                           "target_table  VARCHAR(128),"
                           "inserted      TIMESTAMP DEFAULT NOW(),"
                           "PRIMARY KEY (update_id),"
                           "KEY id (id))")
    except sqlalchemy.exc.InternalError:
        # table already exists
        pass


def wait_until_db_ready(container, attempts=20, delay=2):
    """Checks a running container to see if mysql has started up.

    Args:
        container(:obj:`docker.container`): container to check
        attempts(int): number of attempts to make
        delay(int): seconds to wait between each attempt
    """
    for _ in range(attempts):
        result = container.exec_run('mysqladmin ping --silent')
        if result.exit_code == 0 and bool(result.output) is False:
            return
        else:
            logging.info("Waiting for database to be ready")
            time.sleep(delay)
    raise ConnectionError(f"MYSQL was not ready after {attempts} attempts. Aborting")


@contextmanager
def containerised_database(*, name='luigi-test-runner-db', db_config, config_header):
    """Creates a MYSQL database running in docker and provides a context manager.
    The database is created from scratch each time. When exiting the context manager the
    container is stopped but not removed so any issues can be investigated.

    Args:
        name(str): name to give the container
        db_config(str): environmental variable containing the path to the .config file
        config_header(str): header in the config file
    """
    client = docker.from_env()
    db_config_path = os.environ[db_config]
    config = misctools.get_config(db_config_path, config_header)
    database = 'dev'  # all pipelines are run in test mode

    image = f"mysql:{config['version']}"
    environment = {'MYSQL_ROOT_PASSWORD': config['password'],
                   'MYSQL_DATABASE': database}

    stop_and_remove_container(name)
    container = client.containers.run(image,
                                      detach=True,
                                      name=name,
                                      ports={config['port']: 3306},
                                      environment=environment)
    wait_until_db_ready(container)
    create_luigi_table_updates(db_config, config_header, database)
    yield
    container.stop()


def run_luigi_pipeline(module, **kwargs):
    # collect configuration for excludes, run first and run lasts
    # launches the pipeline
    pass


def luigi_test_runner(start_directory,
                      dockerfile='docker/Dockerfile',
                      branch='dev',
                      luigi_kwargs=None,
                      image_tag='luigi_test_runner:test'):
    """Identifies all Luigi pipelines and runs them in Docker for end-to-end testing.

    Args:
        start_directory(str): path to the folder containing routines
        dockerfile(str): path to the Dockerfile to use to build the image
        branch(str): name or tag of the branch to use when running pipelines
        luigi_kwargs(dict): arguments to pass to Luigi tasks eg date
    """
    logging.info(f"Building docker image on branch: {branch}")
    buildargs = {'GIT_TAG': branch}
    build_docker_image(image_tag, buildargs=buildargs)

    root_tasks = find_root_tasks(start_directory)
    logging.info(f"Found {len(root_tasks)} pipelines to run")

    with containerised_database:
        for task in root_tasks:
            try:
                logging.info(f"Running pipeline: {task}")
                run_luigi_pipeline(task)
            except Exception as e:
                logging.error(e)


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
