CREATE TABLE if not exists stg_calendar_data (
    Name VARCHAR(256),
    MeetingDate VARCHAR(256),
    MeetingTime VARCHAR(256),
    MeetingLocation VARCHAR(256),
    AvailableEnEspanol BOOLEAN,
    calendarId VARCHAR(256),
    uniqueId VARCHAR
)