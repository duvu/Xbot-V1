from datetime import datetime, time, date

from datequarter import DateQuarter


def get_end_of_quarter(year_period, quarter_period):
    """
    get end of day timestamp of the day end-of-quarter
    :param year_period:
    :param quarter_period:
    :return:
    """
    if isinstance(quarter_period, str) and len(quarter_period) == 2:
        quarter_period = int(quarter_period[-1])
    quarter = DateQuarter(year_period, quarter_period)
    end_of_quarter_date = quarter.end_date()
    dt = datetime.combine(end_of_quarter_date, time(23, 59, 59))

    return int(dt.timestamp())


def get_current_quarter_str() -> str:
    """
    Get current quarter string
    :return:
    """
    today = date.today()
    quarter = DateQuarter.from_date(today).quarter()
    return 'Q' + str(quarter)


if __name__ == '__main__':
    print(get_current_quarter_str())
