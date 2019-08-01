import pandas as pd

def month_year_distribution(dt):
    '''month_year_dist
    Frequency of months by year.

    Args:
        dt (:obj:`iter` of :obj:`datetime-like`): A sequence of datetime-like
         objects.

    Returns:
        t (:obj:`pandas.DataFrame`): A frequency table of value counts by month
            (index) and year (columns).
    '''
    dt = pd.DatetimeIndex(dt)
    g = dt.to_frame().groupby(pd.Grouper(freq='M')).count()
    t = g.pivot_table(
            index=g.index.month, columns=g.index.year
            ).fillna(0).astype(int)
    t = t.droplevel(0, axis=1)
    t.index.name = 'Month'
    t.columns.name = 'Year'
    return t
    
def year_distribution(dt):
    '''year_distribution
     Frequency of values by year.
    
    Args:
        dt (:obj:`iter` of :obj:`datetime-like`): A sequence of datetime-like
            objects.

    Returns:
        g (:obj:`pandas.DataFrame`): DataFrame of years (index) and frequency
            (column).

    '''
    dt = pd.DatetimeIndex(dt)
    g = dt.to_frame().groupby(pd.Grouper(freq='Y')).count()
    g.index.name = 'Year'
    g.rename(columns={0: 'Frequency'}, inplace=True)
    g.index = g.index.year
    g = g.reset_index()
    return g

def calendar_day_distribution(dt):
    '''calendar_day_distribution
     Frequency of values by year.
    
    Args:
        dt (:obj:`iter` of :obj:`datetime-like`): A sequence of datetime-like
            objects.

    Returns:
        g (:obj:`pandas.DataFrame`): DataFrame of calendar days (index) and 
            frequency (column).

    '''
    dt = pd.DatetimeIndex(dt)
    g = dt.to_frame().groupby(dt.day).count()
    g.rename(columns={0: 'Frequency'}, inplace=True)
    g.index.name = 'Calendar Day'
    g = g.reset_index()
    return g
