from pandas import DataFrame, read_csv
from datetime import datetime
from schema import stg_calendar_data
from numpy import datetime64


default_time = datetime.strptime("00:00", '%H:%M').time()

class CalendarData:
    source_path: str
    data: DataFrame
    __default_time__: datetime = default_time
    

    def __data_prep__(data):
        def rename_columns():
            data.rename(columns={
                'Name': stg_calendar_data.name,
                'Meeting Date': stg_calendar_data.date,
                'Meeting Time': stg_calendar_data.time,
                'Meeting Location': stg_calendar_data.location,
                'Agenda en espanol': stg_calendar_data.espanol,
            }, inplace=True)
        def create_unique_id():
            at_sep = '@'
            name = data[stg_calendar_data.name].copy()
            loc = data[stg_calendar_data.location].copy()
            date = data[stg_calendar_data.date].copy()
            time = data[stg_calendar_data.time].copy()
            loc = loc.str.replace('"', '')
            loc = loc.str.replace("'", '')
            name = name.str.replace('"', '')
            name = name.str.replace("'", '')
            return (
                name.map(str) + at_sep +
                loc.map(str) + at_sep +
                date.map(str) + at_sep +
                time.map(str) + at_sep)
        rename_columns()
        data.dropna(subset=[stg_calendar_data.name], inplace=True)
        data['uniqueId'] = create_unique_id()
        data[stg_calendar_data.date] = data[stg_calendar_data.date].dt.strftime('%Y-%m-%d')
        data[stg_calendar_data.time] = data[stg_calendar_data.time].dt.strftime('%H:%M:%S')
        return data


    def __init__(self, source_path: str):
        self.source_path = source_path
        self.data = CalendarData.__data_prep__(
            read_csv(source_path, parse_dates=['Meeting Date', 'Meeting Time']))
        self.data['calendarId'] = None


    def generate_end_time(timestr: str):
        is_nat = timestr == 'NaT'
        not_string = not isinstance(timestr, str)
        if not_string or is_nat:
            return CalendarData.__default_time__
        timestr = timestr.split(':')
        timestr[0] = f"{ (int(timestr[0]) + 1) % 24}"
        return ":".join(timestr)
