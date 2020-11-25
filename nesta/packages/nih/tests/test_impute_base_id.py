from unittest import mock

from nesta.packages.nih.impute_base_id import get_base_code
from nesta.packages.nih.impute_base_id import impute_base_id
from nesta.packages.nih.impute_base_id import retrieve_id_ranges
from nesta.packages.nih.impute_base_id import impute_base_id_thread

from nesta.packages.nih.impute_base_id import Projects

PATH = "nesta.packages.nih.impute_base_id.{}"

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


@mock.patch(PATH.format("get_mysql_engine"))
@mock.patch(PATH.format("db_session"))
def test_retrieve_id_ranges(mocked_session_context, mocked_engine):
    session = mocked_session_context().__enter__()
    q = session.query().order_by()
    q.all.return_value = [(0,), (1,), ("1",), (2,), (3,),
                          (5,), (8,), (13,), (21,)]
    id_ranges = retrieve_id_ranges(database="db_name", chunksize=3)
    assert id_ranges == [(0, 2),  # 0 <= x <= 2
                         (2, 8),  # 2 <= x <= 8
                         (8, 21)] # 8 <= x <= 21


@mock.patch(PATH.format("get_mysql_engine"))
@mock.patch(PATH.format("db_session"))
@mock.patch(PATH.format("impute_base_id"))
def test_impute_base_id_thread(mocked_impute_base_id,
                               mocked_session_context, mocked_engine):
    session = mocked_session_context().__enter__()
    impute_base_id_thread(0, 2, 'db_name')
    call_args_list = mocked_impute_base_id.call_args_list
    assert len(call_args_list) == 1
    assert call_args_list[0] == [(session, 0, 2)]
