from pandas import DataFrame, read_csv, isna
from datetime import datetime
from warnings import warn
from schema import fct_calendar_data, stg_calendar_data
from pandas import to_datetime
import configparser

config = configparser.ConfigParser()
config.read('./config.ini')

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


    def convert_to_military_time(hour_part:int):
        return (int(hour_part) + 12) % 24


    def convert_date(datestr: str, pattern: str='%Y-%M-%d') -> str:
        return datetime.strptime(datestr, pattern).date()

    def generate_end_time(timestr: str):
        timestr = timestr.split(':')
        endtime = f"{ (int(timestr[0]) + 1) % 24}:00"
        return ":".join(timestr)

    def generate_event_json(row,
        name_key: str=fct_calendar_data.name,
        time_key: str=fct_calendar_data.time,
        date_key: str=fct_calendar_data.date,
        loc_key: str=fct_calendar_data.location,
        time_zone: str='America/Chicago',
        calendarId: str=fct_calendar_data.id):
        """perform event pre-processing and output a Google Calendar API-compliant JSON file"""
        gen_datetime = lambda d, t: f"{ d }T{ t }"
        start_time = row[time_key]
        end_time = CalendarData.generate_end_time(row[time_key])
        date = row[date_key]
        return {
            'calendarId': row[calendarId],
            'summary': row[name_key],
            'location': row[loc_key],
            'start': {
                'dateTime': gen_datetime(date, start_time),
                'timeZone': time_zone
            },
            'end': {
                'dateTime': gen_datetime(date, end_time),
                'timeZone': time_zone
            },
            'reminders': {
                'useDefault': True,
            },
        }