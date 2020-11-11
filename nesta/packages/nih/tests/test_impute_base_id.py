from unittest import mock

from nesta.packages.nih.impute_base_id import get_base_code
from nesta.packages.nih.impute_base_id import impute_base_id
from nesta.packages.nih.impute_base_id import retrieve_id_ranges
from nesta.packages.nih.impute_base_id import impute_base_id_thread

from nesta.packages.nih.impute_base_id import Projects

def test_get_base_code():
    # Pass regex: return base code
    assert get_base_code("helloworld-1-2-3") == "helloworld"
    assert get_base_code("foobar-11-0-23") == "foobar"

    # Fail regex: return input
    assert get_base_code("foo-bar-2-3") == "foo-bar-2-3"
    assert get_base_code("foo-bar-hello-world") == "foo-bar-hello-world"
    assert get_base_code("foobar-11-0") == "foobar-11-0"
    assert get_base_code("foobar123") == "foobar123"


def test_impute_base_id():

    core_ids = ["helloworld-1-2-3", "foobar-11-0-23", "foo-bar-2-3",
                "foo-bar-hello-world", "foobar123"]
    projects = [Projects(application_id=i, core_project_num=core_id)
                for i, core_id in enumerate(core_ids)]

    session = mock.Mock()
    q = session.query().options().execution_options().filter()
    q.all.return_value = projects

    # Check that the base_project_num has not been imputed yet
    assert all(p.base_core_project_num is None 
               for p in projects)

    # Impute the ids
    impute_base_id(session, from_id=None, to_id=None)
    
    # Check that the base_project_num has been imputed
    imputed_values = [p.base_core_project_num for p in projects]
    expect_values = ["helloworld", "foobar", # <-- Regex passes
                     # Regex fails:
                     "foo-bar-2-3", "foo-bar-hello-world", "foobar123"]
    assert imputed_values == expect_values


def retrieve_id_ranges():
    

def impute_base_id_thread():
    pass
