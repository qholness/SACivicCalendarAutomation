from collections import namedtuple

common_cols =['name', 'date', 'time', 'location', 'espanol', 'id', 'uniqueId']
stg_schema = namedtuple('stg_schema', common_cols)
fct_schema = namedtuple('fct_schema', common_cols)


stg_calendar_data = stg_schema(
    name='Name'
    ,date='MeetingDate'
    ,time='MeetingTime'
    ,location='MeetingLocation'
    ,espanol='AvailableEnEspanol'
    ,id='calendarId'
    ,uniqueId='uniqueId')

fct_calendar_data = fct_schema(
    name='Name'
    ,date='MeetingDate'
    ,time='MeetingTime'
    ,location='MeetingLocation'
    ,espanol='AvailableEnEspanol'
    ,id='calendarId'
    ,uniqueId='uniqueId')