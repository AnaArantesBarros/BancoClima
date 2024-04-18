from ppt.config.parameters import start_date, end_date

def date_range(start_date, end_date, step=timedelta(days=1)):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += step