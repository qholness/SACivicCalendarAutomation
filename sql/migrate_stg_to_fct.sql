INSERT INTO fct_calendar_data
SELECT 
    Name
    ,MeetingDate
    ,MeetingTime
    ,MeetingLocation
    ,"Available En Espanol"
    ,null
    ,uniqueId
FROM stg_calendar_data s
WHERE s.uniqueId NOT IN (
    SELECT uniqueId
    FROM fct_calendar_data)