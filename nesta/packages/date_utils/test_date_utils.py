from nesta.packages.date_utils.date_utils import weekchunks

START_DATE = '1 February 2019'
END_DATE = '31 January 2020'

def test_weekchunks_general():
    chunks = weekchunks(START_DATE, END_DATE)
    assert len(chunks) == 52  # 1 year


def test_weekchunks_less_than_one_week():
    end_date = '4 February 2019'
    assert weekchunks(START_DATE, end_date) == [('2019-02-01', '2019-02-04')]


def test_weekchunks_bad_dates():
    chunks = weekchunks(END_DATE, START_DATE)
    assert len(chunks) == 0


def test_weekchunks_no_end_date():
    many_chunks = weekchunks(START_DATE)
    fewer_chunks = weekchunks(END_DATE)

    # Obviously many_chunks should be longer than fewer_chunks
    assert len(many_chunks) == len(fewer_chunks) + 52  # 1 year longer
    assert len(fewer_chunks) > 10  # True at time of writing, so true forever

    # All except the first chunk of fewer_chunks must be in many_chunks
    assert all(chunk in many_chunks for chunk in fewer_chunks[1:])
    
    
def test_weekchunks_until_days_ago():
    many_chunks = weekchunks(START_DATE)

    # Should be zero to one week fewer chunks
    for i in range(1, 365):
        # There should be e.g. (0 or 1) fewer weeks with i in (0, 7)
        #                      (1 or 2) fewer weeks with i in (7, 14)
        #                      etc
        min_fewer_weeks = i // 7
        fewer_range = (min_fewer_weeks, min_fewer_weeks+1)
        print(fewer_range)
        fewer_chunks = weekchunks(START_DATE, until_days_ago=i)
        assert len(many_chunks) - len(fewer_chunks) in fewer_range
